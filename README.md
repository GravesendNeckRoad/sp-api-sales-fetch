UPD 11/8/2024

CRON job is too unreliable with this choppy API, so I have reverted to a HTTP_trigger. A logic app (workflow pictured in attached jpg file) will trigger the function daily, and direct the retries and consecutive actions such as checking for blob update and sending email. 

Also added methods to handle reports in the case of API failure. Most recent backup report will be generated instead. Logically, if the in-stock inventory API returns failure, then it means a report was already generated within the last 30 minutes, so you can fall back to it and still have near real-time data. This method is more stable than juggling fails/retries, which can have unintended results.

May switch to a durable function in the future IF this method proves unstable. Working fine for now, though. 
