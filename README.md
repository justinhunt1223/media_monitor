# Media Monitor


This script will scan the directories defined in `MEDIA_LOCATIONS` for files with extentions in `MEDIA_EXTENSIONS`.

**No changes are ever made to media files, they are only ever read.**

**Step 1**: Generate the list of files with `MEDIA_EXTENSIONS` in `MEDIA_LOCATIONS`.
- Do not prepend `MEDIA_EXTENSIONS` entries with a dot.
- Entries need to be lowercase.
          
**Step 2**: Scan the files.
- Files are processed alphabetically from the list generated in step 1.
- Compares the file's last modified time with the stored value in the database. If the file has been modified, a new checksum is generated and the file is set to be validated.
- Checksums expire after `MEDIA_UPDATE_CHECKSUM_AFTER_DAYS`, so any file that has a stored checksum older than that is set to be validated as well.
           
**Step 3**: Validate the files.
- Files are processed in order their checksum from step 2 was generated, oldest to newest.
- Using the command `MEDIA_VALIDATE_COMMAND`, test the file for validation. The default command decodes the file and any errors are output the the console. A valid file will have no console output, hence valid. This command, if changed, must return nothing to the console if the file is valid. Output is stripped to exclude whitespace and newlines.
- Checksums are calculated right before and after validation to ensure the file has not changed during validation as some videos can take a bit to check. Files will stay valid as long as the checksum does not change. Checksums are updated if the file is changed or after `MEDIA_UPDATE_CHECKSUM_AFTER_DAYS`.
- If a file changes during validation, it will be re-checked the next time this is run.
          
**Step 4**: Clean the database.
- Checks all filenames in the database to ensure they exist on the filesystem. Records are deleted from the database if they do not exist on the filesystem.
           
**Step 5**: Notify the user.
- Sends an email with the list of invalid files if `EMAIL_SMTP_SERVER` is set.
- Send an update over MQTT if `MQTT_BROKER` is set.

**Notes**:
- I recommend running this script initially with a directory containing only a few files to ensure everything runs for you. After you ensure it runs, I set it up as a cron job. The script utilizes a lock file to prevent multiple processes from running.
- The table this script uses is created if not existing. Deleting the table will require rescanning of all files, which will take a while depending on your media library size. It only uses the `POSTGRES_DATABASE_TABLENAME`, so it can safely be used in a database containing other tables.
- If you enable MQTT and use Home Assistant, disable logging for `sensor.media_monitor_count` unless you like an exessively bloated database. Everytime a file is processed, an update is sent over MQTT. You may also clear out `HOMEASSISTANT_DISCOVERY_TOPIC_COUNT` to not use publish to this entity.
- Sample Home Assistant card:

```
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
```
