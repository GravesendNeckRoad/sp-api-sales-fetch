UPD 11/8/2024

Added methods to handle inventory report in the case of API failure. Most recent backup report will be generated instead.
Logically, if the inventory API returns failure, then it means a report was already generated within the last 30 minutes, so you can fall back to it and still have near real-time data. 

Also switched back to an HTTP trigger. Decided that the logic app will handle the CRON timer, since it is already detecing blob and sending email, may as well integrate everything through there.
