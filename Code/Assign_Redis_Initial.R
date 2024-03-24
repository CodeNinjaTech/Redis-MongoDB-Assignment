###############################################################################
# Preparation
###############################################################################
# Load the library
library("redux")

# Create a connection to the local instance of REDIS
r <- redux::hiredis(redux::redis_config(host = "127.0.0.1", port = "6379"))

# Load csv
emails_sent <- read.csv(file.choose(), header = TRUE, sep=",")
modified_listings <- read.csv(file.choose(), header = TRUE, sep=",")

# Connect to MS SQL Server
# library(DBI)
# string <- paste('driver=SQL Server;server=XXXXXXX;',
#                 'database=BDSA;trusted_connection=true', sep = "")
# con <- dbConnect(odbc::odbc(), .connection_string = string, bigint="integer")
#then build a query and run it
# sqlText <- paste("SELECT * FROM BDSA.dbo.emails_sent_2", sep="")
# emails_sent <- dbGetQuery(con, sqlText)
# sqlText <- paste("SELECT * FROM BDSA.dbo.modified_listings_2", sep="")
# modified_listings <- dbGetQuery(con, sqlText)
# dbDisconnect(con)

# Start clock to measure time taken to run the following script
start_time <- Sys.time()

###############################################################################
# 1.1	How many users modified their listing on January?
###############################################################################
# Create a new vector with the modified listings of January

ModificationsJanuary <- unique(modified_listings[modified_listings$MonthID==1 &
                                      modified_listings$ModifiedListing==1,1])
# Create a loop to assign to a REDIS bitmap the respective values of each user
for (i in 1:length(ModificationsJanuary)){
  r$SETBIT("ModificationsJanuary",ModificationsJanuary[i],1)
}
# Find users who modified their listing in January
r$BITCOUNT("ModificationsJanuary") # 9969

###############################################################################
# 1.2	How many users did NOT modify their listing on January?
###############################################################################
# Perform logical negation on each bit of the previous bitmap
r$BITOP("NOT","NonModificationsJanuary","ModificationsJanuary")
# Find users who did NOT modify their listing on January
r$BITCOUNT("NonModificationsJanuary") # 10031
# Sum users who modified and not modified their listing on January
r$BITCOUNT("ModificationsJanuary") + 
  r$BITCOUNT("NonModificationsJanuary") # 20000
# Find bytes
20000/8
# Get the 20,000th user (do not really exist)
r$GETBIT("NonModificationsJanuary",19999) # 1
# BITOP operations happen at byte-level increments. 19999 bits fit in 
# 2500 bytes = 2500*8 bits = 19999 + 1 bits.
# So the last bit (20,000th) was used, although not depicting a User ID and had
# the value of 0, so when the bit-wise NOT operation was used it performed a 
# logical negation and was turned into 1.

###############################################################################
# 1.3	How many users received at least one e-mail per month (at least one 
# e-mail in January and at least one e-mail in February and at least one e-mail 
# in March)?
###############################################################################
# Create three new vectors with the distinct users that was sent 
# at least one e-mail per month
emails_sent_unique <- unique(emails_sent[c("UserID","MonthID")])
EmailsJanuary <- emails_sent_unique[emails_sent_unique$MonthID==1,1]
EmailsFebruary <- emails_sent_unique[emails_sent_unique$MonthID==2,1]
EmailsMarch <- emails_sent_unique[emails_sent_unique$MonthID==3,1]
# Create three bitmaps filling them with “1”s on the users that was sent email
for (i in 1:length(EmailsJanuary)){
  r$SETBIT("EmailsJanuary",EmailsJanuary[i],1)
}
for (i in 1:length(EmailsFebruary)){
  r$SETBIT("EmailsFebruary",EmailsFebruary[i],1)
}
for (i in 1:length(EmailsMarch)){
  r$SETBIT("EmailsMarch",EmailsMarch[i],1)
}
# Find users who received at least one e-mail per month 
r$BITOP("AND","results_1_3",c("EmailsJanuary","EmailsFebruary","EmailsMarch"))
r$BITCOUNT("results_1_3") # 2668

###############################################################################
# 1.4	How many users received an e-mail on January and March but NOT on 
# February?
###############################################################################
# Perform an inversion of "EmailsFebruary"
r$BITOP("NOT","NoEmailsFebruary","EmailsFebruary")
# Find users who received an e-mail in January and March but NOT in February
r$BITOP("AND","results_1_4",c("EmailsJanuary","NoEmailsFebruary","EmailsMarch"))
r$BITCOUNT("results_1_4") # 2417

###############################################################################
# 1.5	How many users received an e-mail on January that they did not open but 
# they updated their listing anyway?
###############################################################################
# Create a vector containing the users that was sent an email in January and 
# they opened it
EmailsOpenedJanuary <-
unique(emails_sent[(emails_sent$MonthID==1) & (emails_sent$EmailOpened==1),2])
# Create the relevant bitmap in REDIS
for (i in 1:length(EmailsOpenedJanuary)){
  r$SETBIT("EmailsOpenedJanuary",EmailsOpenedJanuary[i],1)
}
# Perform an inversion of previously created bitmap
r$BITOP("NOT","EmailsNotOpenedJanuary","EmailsOpenedJanuary")
# Find users who received an e-mail in January that they did not open but they 
# updated their listing anyway
r$BITOP("AND","results_1_5",c("EmailsJanuary","EmailsNotOpenedJanuary",
                              "ModificationsJanuary"))
r$BITCOUNT("results_1_5") # 1961

###############################################################################
# 1.6	How many users received an e-mail on January that they did not open but 
# they updated their listing anyway on January OR they received an e-mail on 
# February that "they did not open but they updated their listing anyway on 
# February OR they received an e-mail on March that they did not open but they 
# updated their listing anyway on March?
###############################################################################
# Create two vectors containing the users that was sent to them an email that 
# they opened in February and March respectively
EmailsOpenedFebruary <-
  unique(emails_sent[(emails_sent$MonthID==2) & (emails_sent$EmailOpened==1),2])
EmailsOpenedMarch <-
  unique(emails_sent[(emails_sent$MonthID==3) & (emails_sent$EmailOpened==1),2])
# Create the relevant bitmaps in REDIS
for (i in 1:length(EmailsOpenedFebruary)){
  r$SETBIT("EmailsOpenedFebruary",EmailsOpenedFebruary[i],1)
}
for (i in 1:length(EmailsOpenedMarch)){
  r$SETBIT("EmailsOpenedMarch",EmailsOpenedMarch[i],1)
}
# Invert previously created bitmaps
r$BITOP("NOT","EmailsNotOpenedFebruary","EmailsOpenedFebruary")
r$BITOP("NOT","EmailsNotOpenedMarch","EmailsOpenedMarch")
# Create two new vectors with the modified listings of February and March
ModificationsFebruary <- unique(modified_listings[modified_listings$MonthID==2 &
                                  modified_listings$ModifiedListing==1,1])
ModificationsMarch <- unique(modified_listings[modified_listings$MonthID==3 &
                                  modified_listings$ModifiedListing==1,1])
# Create loops to assign to REDIS bitmaps the respective values
for (i in 1:length(ModificationsFebruary)){
  r$SETBIT("ModificationsFebruary",ModificationsFebruary[i],1)
}
for (i in 1:length(ModificationsMarch)){
  r$SETBIT("ModificationsMarch",ModificationsMarch[i],1)
}
# Create bitmaps of each month's users who received an e-mail that they did not 
# open but they updated their listing anyway
r$BITOP("AND","results_1_6_b",c("EmailsFebruary","EmailsNotOpenedFebruary",
                              "ModificationsFebruary"))
r$BITOP("AND","results_1_6_c",c("EmailsMarch","EmailsNotOpenedMarch",
                                "ModificationsMarch"))
# Find the answer using "BITOP OR" and counting "1"s
r$BITOP("OR","results_1_6",c("results_1_5","results_1_6_b","results_1_6_c"))
r$BITCOUNT("results_1_6") # 5249

# Stop clock
end_time <- Sys.time()

###############################################################################
# 1.7	Does it make any sense to keep sending e-mails with recommendations to 
# sellers? Does this strategy really work? How would you describe this in terms 
# a business person would understand?
###############################################################################
# 58.8% of messages sent, without taking account technical issues, were opened
(r$BITCOUNT('EmailsOpenedJanuary') + r$BITCOUNT('EmailsOpenedFebruary') + 
  r$BITCOUNT('EmailsOpenedMarch')) /
  (r$BITCOUNT("EmailsJanuary") + r$BITCOUNT("EmailsFebruary") + 
     r$BITCOUNT("EmailsMarch"))
# 49.9% of messages opened can be linked to respective listing modification, 
# which is linked to the 29.4% of the total messages sent, without taking into
# account technical issues.
r$BITOP("AND","OpenModifyJan",c("EmailsOpenedJanuary","ModificationsJanuary"))
r$BITOP("AND","OpenModifyFeb",c("EmailsOpenedFebruary","ModificationsFebruary"))
r$BITOP("AND","OpenModifyMar",c("EmailsOpenedMarch","ModificationsMarch"))
(r$BITCOUNT("OpenModifyJan") + r$BITCOUNT("OpenModifyFeb") + 
  r$BITCOUNT("OpenModifyMar")) /
  (r$BITCOUNT('EmailsOpenedJanuary') + r$BITCOUNT('EmailsOpenedFebruary') + 
     r$BITCOUNT('EmailsOpenedMarch'))
(r$BITCOUNT("OpenModifyJan") + r$BITCOUNT("OpenModifyFeb") + 
    r$BITCOUNT("OpenModifyMar")) /
  (r$BITCOUNT("EmailsJanuary") + r$BITCOUNT("EmailsFebruary") + 
     r$BITCOUNT("EmailsMarch"))
# However, all listing modifications that can be considered to be a result of 
# the opened emails account for the 28.2% of all listing modifications done, 
# whereas all listing modifications that were done by users who received an 
# e-mail that they did not open or by users that they did not receive an email
# account for the 71.8% of all listing modifications done.
nlistings <- nrow(modified_listings[modified_listings$ModifiedListing==1,])
(r$BITCOUNT("OpenModifyJan") + r$BITCOUNT("OpenModifyFeb") + 
    r$BITCOUNT("OpenModifyMar")) / nlistings
r$BITOP("AND","NotOpenModifyJan",c("EmailsNotOpenedJanuary",
                                   "ModificationsJanuary"))
r$BITOP("AND","NotOpenModifyFeb",c("EmailsNotOpenedFebruary",
                                   "ModificationsFebruary"))
r$BITOP("AND","NotOpenModifyMar",c("EmailsNotOpenedMarch",
                                   "ModificationsMarch"))
(r$BITCOUNT("NotOpenModifyJan") + r$BITCOUNT("NotOpenModifyFeb") + 
  r$BITCOUNT("NotOpenModifyMar")) / nlistings


# Measure time taken to run the script
total_time <- end_time - start_time
print(total_time)

# r$FLUSHALL
