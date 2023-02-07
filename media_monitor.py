#!/usr/bin/python3

import datetime
import glob
import hashlib
import json
import logging
import os
import paho.mqtt.client as mqtt
import psycopg2
import psycopg2.extras
import smtplib
import ssl
import subprocess
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from os import path
from psycopg2 import sql

"""

This script will scan the directories defined in MEDIA_LOCATIONS for files with extentions in MEDIA_EXTENSIONS.
***No changes are made to the media files, they are only ever read.

Step 1: Generate the list of files with MEDIA_EXTENSIONS in MEDIA_LOCATIONS.
          -Do not prepend MEDIA_EXTENSIONS entries with a dot.
          -Entries need to be lowercase.
Step 2: Scan the files.
          -Files are processed alphabetically from the list generated in step 1.
          -Compares the file's last modified time with the stored value in the database. If file has been
           modified, a new checksum is generated and file is set to be validated.
          -Checksums expire after MEDIA_UPDATE_CHECKSUM_AFTER_DAYS, so any file that has a stored checksum
           older than this is set to be validated as well.
Step 3: Validate the files.
          -Files are processed in order their checksum from step 2 was generated, oldest to newest.
          -Using the command MEDIA_VALIDATE_COMMAND, test the file for validation. The default command decodes
           the file and any errors are output the the console. A valid file will have no console output, hence valid.
           This command, if changed, must return nothing to the console if the file is valid. Output is stripped
           to exclude whitespace and newlines.
          -Checksums are calculated right before and after validation to ensure the file has not changed during
           validation as some videos can take a bit to check. Files will stay valid as long as the checksum
           does not change. Checksums are updated if the file is changed or after MEDIA_UPDATE_CHECKSUM_AFTER_DAYS.
          -If a file changes during validation, it will be re-checked the next time this is run.
Step 4: Clean the database.
          -Checks all filenames in the database to ensure they exist on the filesystem. Records are deleted from the
           database if they do not exist on the filesystem.
Step 5: Notify the user.
          -Sends an email with the list of invalid files if EMAIL_SMTP_SERVER is set.
          -Send an update over MQTT if MQTT_BROKER is set.

NOTES
  -I recommend running this script initially with a directory containing only a few files to ensure everything runs for you.
   After you ensure it runs, I set it up as a cron job. The script utilizes a lock file to prevent multiple processes from
   running.
  -The table this script uses is created if not existing. Deleting the table will require rescanning of all files,
   which will take a while depending on your media library size. It only uses the POSTGRES_DATABASE_TABLENAME, so
   it can safely be used in a database containing other tables.
  -If you enable MQTT and use Home Assistant, disable logging for sensor.media_monitor_count unless you like
   an exessively bloated database. Everytime a file is processed, an update is sent over MQTT. You may also
   clear out HOMEASSISTANT_DISCOVERY_TOPIC_COUNT to not use publish to this entity.
  -Sample Home Assistant card:

type: entities
entities:
- entity: sensor.media_monitor
    secondary_info: last-updated
    name: Step
- entity: sensor.media_monitor_percent_done
    secondary_info: last-updated
    name: Step Progress Percent
- entity: sensor.media_monitor_counts
    secondary_info: last-updated
    name: Step Progress Count
- entity: sensor.media_monitor_invalid_count
    secondary_info: last-updated
    name: Invalid Files
title: Media Monitor

"""

#
# CONFIG
#

POSTGRES_HOST: str = "postgres.domain.com"
POSTGRES_USER: str = "media_monitor"
POSTGRES_PASSWORD: str = "media_monitor"
POSTGRES_DATABASE: str = "media_monitor"
POSTGRES_DATABASE_TABLENAME: str = "public.media_monitor" # schema.table format

MEDIA_LOCATIONS: list = [
    "/media/Movies",
    "/media/Music",
    "/media/TV Shows"
]

MEDIA_EXTENSIONS: list = [
    "mp4",
    "mpeg",
    "mov",
    "wmv",
    "avi",
    "mkv",
    "mp3",
    "flac",
    "wma",
    "ogg"
]

# Must contain {filename} without quotes for string replacement; quotes are inserted.
# Command must return no output to be considered a valid file.
MEDIA_VALIDATE_COMMAND: str = "ffmpeg -v error -i {filename} -f null -"
MEDIA_UPDATE_CHECKSUM_AFTER_DAYS: int = 180

MQTT_BROKER: str = "mqtt.domain.com"
MQTT_PORT: int = 1883
MQTT_USERNAME: str = "mqttusername"
MQTT_PASSWORD: str = "mqttpassword"
MQTT_TOPIC_BASE: str = "media_monitor"

HOMEASSISTANT_DISCOVERY_TOPIC_STATUS: str = "homeassistant/sensor/media_monitor/config"
HOMEASSISTANT_DISCOVERY_TOPIC_PERCENT_DONE: str = "homeassistant/sensor/media_monitor_percent_done/config"
HOMEASSISTANT_DISCOVERY_TOPIC_COUNT: str = "homeassistant/sensor/media_monitor_count/config"
HOMEASSISTANT_DISCOVERY_TOPIC_INVALID_COUNT: str = "homeassistant/sensor/media_monitor_invalid_count/config"

EMAIL_SMTP_SERVER: str = "mail.domain.com"
EMAIL_SMTP_PORT: int = 587
EMAIL_SENDER_ADDRESS: str = "from@domain.com"
EMAIL_SENDER_PASSWORD: str = "fromaccountpassword"
EMAIL_RECEIVER_ADDRESS: str = "to@domain.com"

#
# END CONFIG
#

logging.basicConfig(
    format='%(asctime)s %(levelname)-8s %(message)s',
    level=logging.INFO,
    datefmt='%Y-%m-%d %H:%M:%S',
    filename=__file__ + ".log"
)

class Map(dict):

    def __init__(self, *args, **kwargs):
        super(Map, self).__init__(*args, **kwargs)
        for arg in args:
            if isinstance(arg, dict):
                for k, v in arg.iteritems():
                    self[k] = v

        if kwargs:
            for k, v in kwargs.iteritems():
                self[k] = v

    def __getattr__(self, attr):
        return self.get(attr)

    def __setattr__(self, key, value):
        self.__setitem__(key, value)

    def __setitem__(self, key, value):
        super(Map, self).__setitem__(key, value)
        self.__dict__.update({key: value})

    def __delattr__(self, item):
        self.__delitem__(item)

    def __delitem__(self, key):
        super(Map, self).__delitem__(key)
        del self.__dict__[key]

class Database:
    con = None
    cur = None
    mediaMonitor = None

    def __init__(self, mediaMonitor):
        self.con = psycopg2.connect(
            database = POSTGRES_DATABASE,
            user = POSTGRES_USER,
            password = POSTGRES_PASSWORD,
            host = POSTGRES_HOST
        )

        self.mediaMonitor = mediaMonitor

        if not self.con:
            logging.error("Exiting. Failed to connect to database. Ensure database exists and user has permissions to access it.")
            exit()

        self.con.autocommit = True
        self.cur = self.con.cursor(cursor_factory = psycopg2.extras.DictCursor)

        self.assertTableExists()

    def runQuery(self, sqlString: str, parameters: dict = {}, returnFirst: bool = False):
        for k, v in parameters.items():
            if isinstance(v, str):
                parameters[k] = sql.Literal(v)

        sanitizedQuery = sql.SQL(sqlString).format(**parameters)
        self.cur.execute(sanitizedQuery)
        rows = []
        try:
            for record in self.cur.fetchall():
                rows.append(Map(record))
        except psycopg2.ProgrammingError:
            pass

        if returnFirst and rows:
            row = rows[0]
            if len(row) == 1:
                for k, v in row.items():
                    return v

            return rows[0]

        return rows

    def getInvalidFiles(self) -> list:
        query: list = self.runQuery(
            "SELECT filename FROM {table} WHERE is_valid = false ORDER BY filename ASC;",
            {
                "table": sql.SQL(POSTGRES_DATABASE_TABLENAME),
            }
        )
        
        records: list = []
        for record in query:
            records.append(record.filename)
        return records

    def deleteRecord(self, filename: str):
        self.runQuery(
            "DELETE FROM {table} WHERE filename = {filename};",
            {
                "table": sql.SQL(POSTGRES_DATABASE_TABLENAME),
                "filename": filename
            }
        )
        exists: bool = self.runQuery(
            "SELECT EXISTS (SELECT filename FROM {table} WHERE filename = {filename});",
            {
                "table": sql.SQL(POSTGRES_DATABASE_TABLENAME),
                "filename": filename
            },
            True
        )

        if exists:
            raise Exception(f"Failed to delete from database.")

    def getAllFilenames(self) -> list:
        files = self.runQuery(
            "SELECT filename FROM {table};",
            {
                "table": sql.SQL(POSTGRES_DATABASE_TABLENAME)
            }
        )

        filenames: list = []
        for record in files:
            filenames.append(record.filename)
        return filenames

    def getFilesToValidate(self) -> list:
        files: list = self.runQuery(
            "SELECT filename FROM {table} WHERE is_valid IS NULL ORDER BY checksummed_on ASC;",
            {
                "table": sql.SQL(POSTGRES_DATABASE_TABLENAME)
            }
        )

        filenames: list = []
        record: dict = None
        for record in files:
            filenames.append(record.filename)
        return filenames

    def setFileValidity(self, filename: str, isValid: bool) -> bool:
        isValidString: str = ["false", "true"][isValid]
        record = self.runQuery(
            "UPDATE {table} SET is_valid = " + isValidString + ", validated_on = NOW() WHERE filename = {filename} RETURNING *;",
            {
                "table": sql.SQL(POSTGRES_DATABASE_TABLENAME),
                "filename": filename
            }
        )

        if record:
            return True

        raise Exception(f"Failed to set as {isValidString}.")

    def needToUpdateChecksum(self, filename: str, lastModifiedOn: str) -> bool:
        lastModifiedUnchange: str = self.runQuery(
            "SELECT EXISTS (SELECT * FROM {table} WHERE filename = {filename} AND last_modified_on = {last_modified_on});",
            {
                "table": sql.SQL(POSTGRES_DATABASE_TABLENAME),
                "filename": filename,
                "last_modified_on": lastModifiedOn
            },
            True
        )
        if not lastModifiedUnchange:
            return True

        checksumExpired: str = self.runQuery(
            "SELECT EXISTS (SELECT * FROM {table} WHERE filename = {filename} AND checksummed_on <= NOW() - INTERVAL {interval});",
            {
                "table": sql.SQL(POSTGRES_DATABASE_TABLENAME),
                "filename": filename,
                "interval": str(MEDIA_UPDATE_CHECKSUM_AFTER_DAYS) + " days"
            },
            True
        )
        if checksumExpired:
            return True

        return False

    def updateChecksum(self, filename: str):
        lastModifiedOn: str = self.mediaMonitor.getLastModifiedOn(filename)

        if not self.needToUpdateChecksum(filename, lastModifiedOn):
            return

        savedChecksum: str = self.runQuery(
            "SELECT checksum FROM {table} WHERE filename = {filename};",
            {
                "table": sql.SQL(POSTGRES_DATABASE_TABLENAME),
                "filename": filename
            },
            True
        )

        if not savedChecksum:
            inserted = self.runQuery(
                "INSERT INTO {table} (filename, last_modified_on) VALUES ({filename}, {last_modified_on}) RETURNING *;",
                {
                    "table": sql.SQL(POSTGRES_DATABASE_TABLENAME),
                    "filename": filename,
                    "last_modified_on": lastModifiedOn
                }
            )

            if not inserted:
                raise Exception(f"Failed to create record in database.")

        updated = self.runQuery(
            "UPDATE {table} SET checksum = {checksum}, checksummed_on = NOW(), validated_on = NULL, is_valid = NULL, last_modified_on = {last_modified_on} WHERE filename = {filename} RETURNING *;",
            {
                "table": sql.SQL(POSTGRES_DATABASE_TABLENAME),
                "checksum": self.mediaMonitor.getChecksum(filename),
                "last_modified_on": lastModifiedOn,
                "filename": filename
            }
        )

        if not updated:
            raise Exception(f"Failed to update record in database.")

    def assertTableExists(self):
        self.runQuery(
            "CREATE TABLE IF NOT EXISTS {table} (filename TEXT PRIMARY KEY, checksum TEXT, checksummed_on TIMESTAMP WITHOUT TIME ZONE, validated_on TIMESTAMP WITHOUT TIME ZONE, is_valid BOOL, last_modified_on TIMESTAMP WITHOUT TIME ZONE NOT NULL);",
            {
                "table": sql.SQL(POSTGRES_DATABASE_TABLENAME)
            }
        )

        doesTableExists: bool = self.runQuery(
            "SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_schema = {schema} AND table_name = {table});",
            {
                "schema": POSTGRES_DATABASE_TABLENAME.split(".")[0],
                "table": POSTGRES_DATABASE_TABLENAME.split(".")[1]
            },
            True
        )

        if not doesTableExists:
            logging.error("Exiting. Failed to create table, check permissions.")
            exit()

class Mqtt:
    client: mqtt.Client = None

    def __init__(self):
        if not MQTT_BROKER:
            return
        
        try:
            self.client = mqtt.Client("media_monitor")
            self.client.username_pw_set(MQTT_USERNAME, MQTT_PASSWORD)
            self.client.will_set(f"{MQTT_TOPIC_BASE}/status", "offline")
            self.client.connect(MQTT_BROKER, MQTT_PORT)
            self.client.loop_start()

            self.updateStatus("init")
            self.updateCount(0, 0)
        except Exception as e:
            logging.error(f"MQTT is enabled but failed to initialize: {str(e)}")
            exit()

    def send(self, topic: str, data: str):
        self.client.publish(topic, data, qos=2)

    def sendDiscovery(self):
        data: dict = {
            "name": "Media Monitor",
            "state_topic": f"{MQTT_TOPIC_BASE}/state",
            "availability_topic": f"{MQTT_TOPIC_BASE}/status",
            "icon": "mdi:multimedia"
        }
        self.send(
            HOMEASSISTANT_DISCOVERY_TOPIC_STATUS,
            json.dumps(data)
        )

        data["name"] = "Media Monitor: Percent Done"
        data["state_topic"] = f"{MQTT_TOPIC_BASE}/percent_done"
        data["icon"] = "mdi:progress-wrench"
        data["unit_of_measurement"] = "%"
        self.send(
            HOMEASSISTANT_DISCOVERY_TOPIC_PERCENT_DONE,
            json.dumps(data)
        )

        if HOMEASSISTANT_DISCOVERY_TOPIC_COUNT:
            data["name"] = "Media Monitor: Counts"
            data["state_topic"] = f"{MQTT_TOPIC_BASE}/count"
            data["icon"] = "mdi:image-multiple-outline"
            del data["unit_of_measurement"]
            self.send(
                HOMEASSISTANT_DISCOVERY_TOPIC_COUNT,
                json.dumps(data)
            )

        data["name"] = "Media Monitor: Invalid Count"
        data["state_topic"] = f"{MQTT_TOPIC_BASE}/invalid_count"
        data["icon"] = "mdi:file-alert-outline"
        del data["availability_topic"]
        self.send(
            HOMEASSISTANT_DISCOVERY_TOPIC_INVALID_COUNT,
            json.dumps(data)
        )

    def updateStatus(self, text: str):
        if not self.client:
            return

        self.sendDiscovery()
        self.send(f"{MQTT_TOPIC_BASE}/status", "online")
        self.send(
            f"{MQTT_TOPIC_BASE}/state",
            text
        )

    def updateInvalidCount(self, count: int):
        self.sendDiscovery()
        self.send(f"{MQTT_TOPIC_BASE}/status", "online")

        self.send(
            f"{MQTT_TOPIC_BASE}/invalid_count",
            count
        )

    def updateCount(self, index: int, count: int):
        if not self.client:
            return

        if not HOMEASSISTANT_DISCOVERY_TOPIC_COUNT:
            return

        self.sendDiscovery()
        self.send(f"{MQTT_TOPIC_BASE}/status", "online")

        percentDone: int = 0
        if index:
            percentDone = int(float(index) / float(count) * 100)

        self.send(
            f"{MQTT_TOPIC_BASE}/percent_done",
            percentDone
        )

        self.send(
            f"{MQTT_TOPIC_BASE}/count",
            f"{index} / {count}"
        )

class MediaMonitor:
    db: Database = None
    mqtt: Mqtt = None

    files: list = []

    actions: list = []
    actionsIndex: int = 0
    actionsCount: int = 0

    processIndex: int = 0
    processCount: int = 0

    lockFilename: str = None

    def __init__(self):
        logging.info("Starting")

        self.assertLock()

        self.db = Database(self)
        self.mqtt = Mqtt()

        self.actions.append(self.generateFileList)
        self.actions.append(self.scanFiles)
        self.actions.append(self.checkFiles)
        self.actions.append(self.cleanDatabase)
        self.actions.append(self.processInvalids)

        self.actionsCount = len(self.actions)

        for action in self.actions:
            self.actionsIndex += 1
            self.processIndex = 0
            self.processCount = 0
            self.mqtt.updateCount(self.processIndex, self.processCount)
            action()

        self.mqtt.updateStatus("Done")
        self.mqtt.updateCount(0, 0)
        self.clearLock()
        exit()

    def assertLock(self):
        self.lockFilename = __file__ + ".lock"

        if path.exists(self.lockFilename):
            lockFile = open(self.lockFilename, "r")
            pid = lockFile.readline()
            try:
                os.kill(int(pid), 0)
            except OSError:
                self.clearLock()
                self.assertLock()
                return
            else:
                logging.info(f"Exiting. Previous process is still running.")
                exit()
        else:
            lockFile = open(self.lockFilename, "w")
            lockFile.write(
                str(os.getpid())
            )
            lockFile.close()

    def clearLock(self):
        os.remove(self.lockFilename)

        if path.exists(self.lockFilename):
            logging.error(f"Exiting. Failed to delete lock file.")

    def generateFileList(self):
        self.mqtt.updateStatus(f"({self.actionsIndex}/{self.actionsCount}) Generating list of files")
        for directory in MEDIA_LOCATIONS:
            directory = directory.rstrip("/") + "/**/*.*"
            for filename in glob.iglob(directory, recursive=True):
                f, extension = os.path.splitext(filename.lower())
                if not extension.lstrip(".") in MEDIA_EXTENSIONS:
                    continue

                self.files.append(filename)

        self.files.sort()

    def scanFiles(self):
        self.processIndex = 0
        self.processCount = len(self.files)
        for filename in self.files:
            self.processIndex += 1
            self.mqtt.updateStatus(f"({self.actionsIndex}/{self.actionsCount}) Checksumming files")
            self.mqtt.updateCount(self.processIndex, self.processCount)
            try:
                self.db.updateChecksum(filename)
            except Exception as e:
                logging.error(f"Failed to checksum file {filename}: {str(e)}")

    def checkFiles(self):
        self.files = self.db.getFilesToValidate()
        self.processIndex = 0
        self.processCount = len(self.files)
        for filename in self.files:
            self.processIndex += 1
            self.mqtt.updateStatus(f"({self.actionsIndex}/{self.actionsCount}) Processing files")
            self.mqtt.updateCount(self.processIndex, self.processCount)
            if not path.exists(filename):
                continue
                
            try:
                isValid: bool = False
                preChecksum: str = self.getChecksum(filename)
                process = subprocess.run(
                        f"{MEDIA_VALIDATE_COMMAND}".replace(
                            "{filename}",
                            '"' + filename + '"'
                        ),
                        capture_output=True,
                        shell=True
                    )
                output = process.stdout.decode("utf-8").strip() + process.stderr.decode("utf-8").strip()
                postChecksum: str = self.getChecksum(filename)
                if preChecksum != postChecksum:
                    raise Exception(f"File changed during validation.")
                
                if output == "":
                    isValid = True

                self.db.setFileValidity(filename, isValid)

                if not isValid:
                    self.mqtt.updateInvalidCount(
                        len(
                            self.db.getInvalidFiles()
                        )
                    )
            except Exception as e:
                logging.error(f"Failed to validate file {filename}: {str(e)}")

    def cleanDatabase(self):
        self.files = self.db.getAllFilenames()
        self.processIndex = 0
        self.processCount = len(self.files)
        for filename in self.files:
            self.processIndex += 1
            self.mqtt.updateStatus(f"({self.actionsIndex}/{self.actionsCount}) Cleaning database")
            self.mqtt.updateCount(self.processIndex, self.processCount)

            if not path.exists(filename):
                self.db.deleteRecord(filename)

    def processInvalids(self):
        self.files = self.db.getInvalidFiles()
        self.processIndex = 0
        self.processCount = len(self.files)
        self.mqtt.updateStatus(f"({self.actionsIndex}/{self.actionsCount}) Writing output")
        self.mqtt.updateInvalidCount(self.processCount)

        if self.files and EMAIL_SMTP_SERVER:
            html: str = "<html><body><table><thead><tr><th>Filename</th></tr></thead><tbody>"
            for filename in self.files:
                html = html + f"<tr><td>{filename}</td></tr>"
            html = html + "</tbody></table></body></html>"

            message: MIMEMultipart = MIMEMultipart("alternative")
            message["Subject"] = f"Media Monitor Results: {self.processCount} Invalid"
            message["From"] = EMAIL_SENDER_ADDRESS
            message["To"] = EMAIL_RECEIVER_ADDRESS

            message.attach(
                MIMEText(html, "html")
            )

            with smtplib.SMTP(EMAIL_SMTP_SERVER, EMAIL_SMTP_PORT) as server:
                server.starttls(context=ssl.create_default_context())
                server.login(
                    EMAIL_SENDER_ADDRESS,
                    EMAIL_SENDER_PASSWORD
                )
                server.sendmail(
                    EMAIL_SENDER_ADDRESS,
                    EMAIL_RECEIVER_ADDRESS,
                    message.as_string()
                )

    def getChecksum(self, filename: str) -> str:
        if not path.exists(filename):
            raise Exception(f"File does not exist.")

        try:
            hash = hashlib.md5()
            with open(filename, "rb") as f:
                while chunk := f.read(8192):
                    hash.update(chunk)
        
            return hash.hexdigest()
        except:
            raise Exception(f"Failed to get checksum for file.")

    def getLastModifiedOn(self, filename: str) -> str:
        if not path.exists(filename):
            raise Exception(f"File does not exist.")

        lastModifedTimestamp: int = path.getmtime(filename)
        lastModifiedOn = datetime.datetime.fromtimestamp(lastModifedTimestamp).strftime("%Y-%m-%d %H:%M:%S")
        if not lastModifiedOn:
            raise Exception(f"Failed to get last modified time.")

        return lastModifiedOn

mediaMonitor = MediaMonitor()
