# Load the libraries
library('RcppRedis')

# Create a connection to the local instance of REDIS
redis <- new(Redis, "localhost")

# Start clock to measure time taken to load and clean data in R
R_time <- Sys.time()

# Load csv
emails_sent <- read.csv(file.choose(), header = TRUE, sep=",")
modified_listings <- read.csv(file.choose(), header = TRUE, sep=",")
# Cleaning
emails_sent <- unique(emails_sent[,c(2,3,4)])
modified_listings <- unique(modified_listings[
  modified_listings$ModifiedListing==1,c(1,2)])

# Connect to MS SQL Server
# library(DBI)
# string <- paste('driver=SQL Server;server=XXXXXXX;',
#                 'database=BDSA;trusted_connection=true', sep = "")
# con <- dbConnect(odbc::odbc(), .connection_string = string, bigint="integer")
# # then build a query and run it
# sqlText <- paste("SELECT DISTINCT UserID, MonthID, EmailOpened FROM ",
#                  "BDSA.dbo.emails_sent_2 ORDER BY UserID", sep="")
# emails_sent <- dbGetQuery(con, sqlText)
# sqlText <- paste("SELECT DISTINCT UserID, MonthID FROM ",
#                  "BDSA.dbo.modified_listings_2 as a ",
#                  "WHERE a.ModifiedListing = 1 ", 
#                  "ORDER BY UserID", sep="")
# modified_listings <- dbGetQuery(con, sqlText)
# dbDisconnect(con)

# Function to create REDIS BITMAPS
Create.Redis.Bitmap <- function(subset, bitmapName) {
  comment <- paste('SETBIT', bitmapName, max(subset), '1', sep=" ")
  redis$exec(comment)
  for (i in seq(1, length(subset), by=200000)) {
    if (i+199999 < length(subset)){
      n <- i+199999
    } else {
      n <- length(subset)
    }
    comment <- subset[i:n]
    comment <- paste('SET u1', comment, '1', sep=" ", collapse=" ")
    comment <- paste('BITFIELD', bitmapName, comment)
    redis$exec(comment)
  }
}

# Start clock to measure time taken to load data to REDIS
Redis_time <- Sys.time()

# Create REDIS BITMAP ModificationsJanuary
subset <- modified_listings[modified_listings$MonthID==1,1]
Create.Redis.Bitmap(subset, "ModificationsJanuary")
# Create REDIS BITMAP ModificationsFebruary
subset <- modified_listings[modified_listings$MonthID==2,1]
Create.Redis.Bitmap(subset, "ModificationsFebruary")
# Create REDIS BITMAP ModificationsMarch
subset <- modified_listings[modified_listings$MonthID==3,1]
Create.Redis.Bitmap(subset, "ModificationsMarch")

# Free no longer needed R memory
rm("modified_listings")
gc()

# Create subset of emails sent in January
subsetM <- emails_sent[emails_sent$MonthID==1,c(1,3)]
# Create REDIS BITMAP EmailsJanuary
subset <- subsetM[,1]
Create.Redis.Bitmap(subset, "EmailsJanuary")
# Create REDIS BITMAP EmailsOpenedJanuary
subset <- subsetM[subsetM$EmailOpened==1,1]
Create.Redis.Bitmap(subset, "EmailsOpenedJanuary")

# Create subset of emails sent in February
subsetM <- emails_sent[emails_sent$MonthID==2,c(1,3)]
# Create REDIS BITMAP EmailsFebruary
subset <- subsetM[,1]
Create.Redis.Bitmap(subset, "EmailsFebruary")
# Create REDIS BITMAP EmailsOpenedFebruary
subset <- subsetM[subsetM$EmailOpened==1,1]
Create.Redis.Bitmap(subset, "EmailsOpenedFebruary")

# Create subset of emails sent in March
subsetM <- emails_sent[emails_sent$MonthID==3,c(1,3)]

# Free no longer needed R memory
rm("emails_sent")
gc()

# Create REDIS BITMAP EmailsMarch
subset <- subsetM[,1]
Create.Redis.Bitmap(subset, "EmailsMarch")
# Create REDIS BITMAP EmailsOpenedMarch
subset <- subsetM[subsetM$EmailOpened==1,1]
Create.Redis.Bitmap(subset, "EmailsOpenedMarch")

# Free no longer needed R memory
rm("subsetM", "subset")
gc()


# Start measuring REDIS bit operations
Proc_time <- Sys.time()

# Find users who modified their listing in January
redis$exec("BITCOUNT ModificationsJanuary") # 9969

# Perform logical negation on each bit of the previous bitmap
redis$exec("BITOP NOT NonModificationsJanuary ModificationsJanuary")
# Find users who did NOT modify their listing on January
redis$exec("BITCOUNT NonModificationsJanuary") # 10031

# Find users who received at least one e-mail per month 
redis$exec("BITOP AND results_1_3 EmailsJanuary EmailsFebruary EmailsMarch")
redis$exec("BITCOUNT results_1_3") # 2668

# Perform an inversion of "EmailsFebruary"
redis$exec("BITOP NOT NoEmailsFebruary EmailsFebruary")
# Find users who received an e-mail in January and March but NOT in February
redis$exec("BITOP AND results_1_4 EmailsJanuary NoEmailsFebruary EmailsMarch")
redis$exec("BITCOUNT results_1_4") # 2417

# Perform an inversion of previously created bitmap
redis$exec("BITOP NOT EmailsNotOpenedJanuary EmailsOpenedJanuary")
# Find users who received an e-mail in January that they did not open but they 
# updated their listing anyway
redis$exec("BITOP AND results_1_5 EmailsJanuary EmailsNotOpenedJanuary ModificationsJanuary")
redis$exec("BITCOUNT results_1_5") # 1961

# Invert previously created bitmaps
redis$exec("BITOP NOT EmailsNotOpenedFebruary EmailsOpenedFebruary")
redis$exec("BITOP NOT EmailsNotOpenedMarch EmailsOpenedMarch")
# Create bitmaps of each month's users who received an e-mail that they did not 
# open but they updated their listing anyway
redis$exec("BITOP AND results_1_6_b EmailsFebruary EmailsNotOpenedFebruary ModificationsFebruary")
redis$exec("BITOP AND results_1_6_c EmailsMarch EmailsNotOpenedMarch ModificationsMarch")
# Find the answer using "BITOP OR" and counting "1"s
redis$exec("BITOP OR results_1_6 results_1_5 results_1_6_b results_1_6_c")
redis$exec("BITCOUNT results_1_6") # 5249

# Stop clock
end_time <- Sys.time()
# Measure time taken to run the script
total_R_load_time <- Redis_time - R_time
print(total_R_load_time)

total_redis_load_time <- Proc_time - Redis_time
print(total_redis_load_time)

total_proc_time <- end_time - Proc_time
print(total_proc_time)

# Flush memory held by REDIS
# redis$exec('FLUSHALL')
