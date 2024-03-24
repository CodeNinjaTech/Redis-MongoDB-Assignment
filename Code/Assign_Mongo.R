###############################################################################
# CREATE A DATA FRAME FROM JSON FILES
###############################################################################

# Load jsonlite
library("jsonlite")

# Save JSON to a variable
index_file_path <- paste('path/to/folder', sep="")
index_file <- paste(index_file_path, 'files_list.txt', sep = "/")
index_file <- readLines(index_file)
f <- file.path(index_file_path, index_file)
json_data <- lapply(f, readLines, encoding="UTF-8")

# Replace empty JSON objects or arrays with null
json_data <- gsub("{}", "null", json_data, perl=TRUE)
json_data <- gsub("[[]]", "null", json_data)

# Replace empty name of Tel field of add_seller field object
json_data <- gsub('\"\":', '\"Tel\":', json_data)

# Check it is OK
json_data[3]

# Number of JSON objects in the list
n <- length(json_data)

# Create empty list
l <- list()

# From character vector save all JSONs in a list of data frames
for (i in 1:n){
  lj <- fromJSON(json_data[i], flatten = TRUE, simplifyDataFrame=FALSE)
  fne <- Filter(Negate(purrr::is_empty), lj)
  df <- as.data.frame(fne)
  l[[length(l)+1]] <- df[1,]
}

# Create a single data frame from the list of data frames
ldf <- dplyr::bind_rows(l, .id = "column_label")

# Have a look at the data
View(ldf)

###############################################################################
# CLEAN THE DATA FRAME BEFORE IMPORTING IT TO MONGODB
###############################################################################

# ad_data.Mileage
ldf$ad_data.Mileage <- gsub(" km", "", ldf$ad_data.Mileage)
ldf$ad_data.Mileage <- gsub(",", "", ldf$ad_data.Mileage)
ldf$ad_data.Mileage <- as.integer(ldf$ad_data.Mileage)

# ad_data.Price
ldf$ad_data.Price <- gsub("€", "", ldf$ad_data.Price)
ldf$ad_data.Price <- gsub("[.]", "", ldf$ad_data.Price)
ad_data.Ask_for_price <- rep(ldf$ad_data.Price)
ad_data.Ask_for_price[ad_data.Ask_for_price!='Askforprice'] <- 0
ad_data.Ask_for_price[ad_data.Ask_for_price=='Askforprice'] <- 1
ad_data.Ask_for_price <- as.integer(ad_data.Ask_for_price)
ldf[ldf$ad_data.Price == 'Askforprice','ad_data.Price'] <- NA
ldf$ad_data.Price <- as.numeric(ldf$ad_data.Price)
ldf <- cbind(ldf, ad_data.Ask_for_price)

# column_label
ldf$column_label <- as.integer(ldf$column_label)

# ad_id
ldf$ad_id <- as.integer(ldf$ad_id)

# ad_data.Classified.number
if (sum(as.integer(ldf$ad_data.Classified.number)!=ldf$ad_id)==0){
  ldf <- ldf[,!(names(ldf) %in% 'ad_data.Classified.number')]
} else {
  ldf$ad_data.Classified.number <- as.integer(ldf$ad_data.Classified.number)
}

# ad_data.Registration
index <- grep('^[1-9]{1} /', ldf$ad_data.Registration)
ldf[index,'ad_data.Registration'] <- 
  paste('0', ldf[index,'ad_data.Registration'], sep = "")
ldf$ad_data.Registration <- gsub(" ", "", ldf$ad_data.Registration)
ldf$ad_data.Registration <- paste('01/',ldf$ad_data.Registration, sep = "")
ldf$ad_data.Registration <- 
  as.Date(ldf$ad_data.Registration, format = "%d/%m/%Y")

# ad_data.Cubic.capacity
ldf$ad_data.Cubic.capacity <- gsub(" cc", "", ldf$ad_data.Cubic.capacity)
ldf$ad_data.Cubic.capacity <- gsub(",", "", ldf$ad_data.Cubic.capacity)
ldf$ad_data.Cubic.capacity <- as.integer(ldf$ad_data.Cubic.capacity)
names(ldf)[names(ldf)=='ad_data.Cubic.capacity'] <- "ad_data.Cubic.capacity_cc"

# ad_data.Power
ldf$ad_data.Power
ldf$ad_data.Power <- gsub(" bhp", "", ldf$ad_data.Power)
ldf$ad_data.Power <- as.integer(ldf$ad_data.Power)
names(ldf)[names(ldf)=='ad_data.Power'] <- "ad_data.Power_bhp"

# ad_data.Previous.owners
ldf$ad_data.Previous.owners <- as.integer(ldf$ad_data.Previous.owners)

# ad_data.Times.clicked
ldf$ad_data.Times.clicked <- as.integer(ldf$ad_data.Times.clicked)

# ad_data.Telephone
ldf[ldf$ad_data.Telephone == "None",'ad_data.Telephone'] <- NA

# ad_data.Kteo.to
ldf$ad_data.Kteo.to
ldf$ad_data.Kteo.to <- gsub(" ", "", ldf$ad_data.Kteo.to)
ldf[!is.na(ldf$ad_data.Kteo.to),'ad_data.Kteo.to'] <- 
  paste('01/',ldf[!is.na(ldf$ad_data.Kteo.to),'ad_data.Kteo.to'], sep = "")
ldf$ad_data.Kteo.to <- as.Date(ldf$ad_data.Kteo.to, format = "%d/%m/%Y")

# metadata.brand & metadata.model
library(stringr)
index <- grep('Negotiable', ldf$metadata.model)
ldf$metadata.Negotiable <- 0
ldf[index, 'metadata.Negotiable'] <- 1
ldf$metadata.model <- 
str_split_fixed(ldf$metadata.model, " - € ", 2)[,1]
ldf$metadata.model <-
str_split_fixed(ldf$metadata.model, " - Ask", 2)[,1]
ldf$metadata.model <- gsub("['][0-9]{2}", "", ldf$metadata.model)
ldf$metadata.brand <- gsub("Χ-Μotors", "X-Motors", ldf$metadata.brand)
ldf$metadata.model <- gsub("Χ-Μotors", "X-Motors", ldf$metadata.model)
ldf$metadata.model <- gsub("[Α-ώ]", "", ldf$metadata.model)
ldf$metadata.brand <- toupper(ldf$metadata.brand)
ldf$metadata.model <- toupper(ldf$metadata.model)
ldf$metadata.model <- gsub("I[.]E", "", ldf$metadata.model)
ldf$metadata.model <- gsub("#MOTO HARRIS!", "", ldf$metadata.model)
ldf$metadata.model <- gsub("[*'\"!.#,◆]", "", ldf$metadata.model)
ldf$metadata.model <- gsub("\\(.*?\\)", "", ldf$metadata.model)
index <- grep("GOCCIA", ldf$metadata.model)
index2 <- grep("TCC", ldf$metadata.model)
ldf$metadata.model <- gsub("CC", "", ldf$metadata.model)
ldf[index,"metadata.model"] <- "GOCCIA 50"
ldf[index2, "metadata.model"] <- "TCC"
for (i in 1:n){
  ldf$metadata.model[i] <- 
    gsub(ldf$metadata.brand[i], "", ldf$metadata.model[i])
}
ldf$metadata.model <- str_squish(ldf$metadata.model)
ldf$metadata.model <- str_trim(ldf$metadata.model)
ldf[(ldf$metadata.model == ""), "metadata.model"] <- NA
brands.models <- 
  unique(ldf[!is.na(ldf$metadata.model),c("metadata.brand", "metadata.model")])
brands.models <- brands.models[order(brands.models[,1], brands.models[,2]),]

###############################################################################
# LOAD DATA FRAME TO MONGODB
###############################################################################

# Load mongolite
library("mongolite")

# Open a connection to MongoDB
m <- mongo(collection = "ads",  db = "mydb", url = "mongodb://localhost")

# Insert this JSON object to MongoDB
m$insert(ldf)

# Check if it has been inserted
m$find('{}')

###############################################################################
# ANSWER SUBTASKS
###############################################################################

# 2.2	How many bikes are there for sale? ######################################

m$count() # 29701


# 2.3	What is the average price of a motorcycle (give a number)? ##############
# What is the number of listings that were used in order to calculate this ####
# average (give a number as well)? Is the number of listings used the same ####
# as the answer in 2.2? Why? ##################################################

query <- '[{"$group":{"_id":{},"Avg_Price":{"$avg":"$ad_data_Price"}}}]'
m$aggregate(query) # 2962.701
query <- '[{"$group":{"_id":{},"Sum_Price":{"$sum":"$ad_data_Price"}}}]'
m$aggregate(query) # 86347921
query <- '[{"$project":{"ad_data_Price":{"$type":"$ad_data_Price"},"_id":0}},
{"$match":{"ad_data_Price":{"$eq":"double"}}},
{"$group":{"_id":{},"Count_Listings_Used":{"$sum":1}}}]'
m$aggregate(query) # 29145
86347921/29145 # 2962.701
29701-29145 # 556
query <- '[{"$match":{"ad_data_Price":null}},
{"$group":{"_id":{},"Count_Ads_NA_Price":{"$sum":1}}}]'
m$aggregate(query) # 556

# $avg ignores non-numeric values (here the null values).


# 2.4	What is the maximum and minimum price of a motorcycle currently #########
# available in the market? ####################################################

query <- '[{"$group":{"_id":{},"Max_Price":{"$max":"$ad_data_Price"},
"Min_Price":{"$min":"$ad_data_Price"}}}]'
m$aggregate(query) # max = 89,000 € and min = 1 €
m$find('{"ad_data_Price":89000}',
       '{"Make_Model":"$ad_data_Make_Model","Price":"$ad_data_Price","_id":0}')

# Price of 89,000€ makes sense because it is for BMW HP4 whose price is close 
# to this. But 1€ does not make sense for any motorcycle. Prices up until 50€
# is for sure for spare parts and not for an entire motorcycle.

m$find('{"ad_data_Make_Model" : "Yamaha JOG \'95","ad_data_Price" : 150}',
'{"URL" : "$query_url","Make_Model" : "$ad_data_Make_Model",
"Price" : "$ad_data_Price","Description" : "$description","_id" : 0}')

# With an exhaustive search, we found the motorcycles above, which were already 
# sold. The prices make sense as there exists a similar bike with a price of 
# 250€ (https://www.car.gr/classifieds/bikes/view/325220425-yamaha-jog) and 
# they were sold entire and not as parts, which can be viewed by the
# description. When they were online, they were a bargain. So, 150€ is the min.


# 2.5 How many listings have a price that is identified as negotiable? ########

# Query if the information of Negotiable ad prices were, as initially, 
# on column metadata_model
query <- '[{"$match":{"metadata_model":{"$regex":"Negotiable","$options":"i"}}},
{"$group":{"_id":{},"Negotiable":{"$sum":1}}}]'

# Query with information of Negotiable ad prices on created column 
# metadata_Negotiable
query <- '[{"$group":{"_id":{},"Negotiable_Ads":{"$sum":
"$metadata_Negotiable"}}}]'

m$aggregate(query) # 1348


# 2.6	(Optional) For each Brand, what percentage of its listings is listed ####
# as negotiable? ##############################################################
# For the answer we have also included the number of ads per brand for the user
# to be able to assess high or low values of percentage

# Method with information of negotiable ads on initial column (harder)

# Create new collection with number of negotiable listings per brand
query <- '[{"$match":{"metadata_model":{"$regex":"Negotiable","$options":"i"}}},
{"$group":{"_id":{"metadata_brand":"$metadata_brand"},"negotiable_ads":
{"$sum":1}}},{"$project":{"metadata_brand":"$_id.metadata_brand",
"negotiable_ads":"$negotiable_ads","_id":0}},{"$out":"alpha"}]'
# m$aggregate(query)
# Answer the question
query <- '[{"$group":{"_id":{"metadata_brand":"$metadata_brand"},"all_ads":
{"$sum":1}}},{"$project":{"metadata_brand":"$_id.metadata_brand","all_ads":
"$all_ads","_id":0}},{"$lookup":{"localField":"metadata_brand","from":"alpha",
"foreignField":"metadata_brand","as":"brandjoin"}},{"$unwind":{"path":
"$brandjoin","preserveNullAndEmptyArrays":true}},{"$project":{"metadata_brand":
1,"percentage_%":{"$round":[{"$multiply":[{"$divide":[{"$ifNull":
["$brandjoin.negotiable_ads",0]},"$all_ads"]},100]},1]},"_id":0}},
{"$sort":{"metadata_brand":1}}]'
# m$aggregate(query)
# Drop created collection
# a <- mongo(collection = "alpha",  db = "mydb", url = "mongodb://localhost")
# a$drop()

# Method with information of negotiable ads on new column (easier)

query <- '[{"$group":{"_id":{"Brand":"$metadata_brand"},
"Negotiable_ads_Percentage":{"$avg":"$metadata_Negotiable"},
"Negotiable_ads":{"$sum":"$metadata_Negotiable"},"Total_ads":
{"$sum":1}}},{"$project":{"Brand":"$_id.Brand",
"Negotiable_ads_Percentage":{"$round":[{"$multiply":
["$Negotiable_ads_Percentage",100]},1]},"Negotiable_ads":{"$toInt":
"$Negotiable_ads"},"Total_ads":"$Total_ads","_id":0}},
{"$sort":{"Brand":1}}]'
m$aggregate(query)


# 2.7	(Optional) What is the motorcycle brand with the ########################
# highest average price? ######################################################

# If by motorcycle brand, ATVs or else Buggy Motorcycles are included
query <- '[{"$group":{"_id":{"Brand":"$metadata_brand"},
"Average_Price":{"$avg":"$ad_data_Price"}}},{"$project":{"Brand":
"$_id.Brand","Average_Price":"$Average_Price","_id":0}},
{"$sort":{"Average_Price":-1}},{"$project":{"_id":0,"Brand":
"$Brand"}},{"$limit":1}]'
# If by motorcycle brand, ATVs or else Buggy Motorcycles are not included
query <- '[{"$match":{"ad_data_Category":{"$ne":"Bike - Βuggy"}}},
{"$group":{"_id":{"Brand":"$metadata_brand"},
"Average_Price":{"$avg":"$ad_data_Price"}}},{"$project":{"Brand":
"$_id.Brand","Average_Price":"$Average_Price","_id":0}},
{"$sort":{"Average_Price":-1}},{"$project":{"_id":0,"Brand":
"$Brand"}},{"$limit":1}]'
m$aggregate(query)

# 2.8	(Optional) What are the TOP 10 models with the highest average age? #####
# (Round age by one decimal number) ###########################################

query <- '[{"$group":{"_id":{"metadata_model":{"$concat":[{"$cond":{"if":{"$eq":
["$metadata_brand","ΑΛΛΟ"]},"then":"","else":"$metadata_brand"}}," ",
"$metadata_model"]}},"avg_age":{"$avg":{"$subtract":[2023,{"$year":{"$toDate":
"$ad_data_Registration"}}]}}}},{"$project":{"metadata_model":
"$_id.metadata_model","avg_age":1,"_id":0}},{"$group":{"_id":{"avg_age":
{"$round":["$avg_age",1]}},"metadata_brands":{"$push":"$metadata_model"}}},
{"$project":{"avg_age":"$_id.avg_age","metadata_model":{"$reduce":{"input":
{"$sortArray":{"input":"$metadata_brands","sortBy":1}},"initialValue":"","in":
{"$concat":["$$value",{"$cond":{"if":{"$eq":["$$value",""]},"then":"","else":
"; "}},"$$this"]}}},"_id":0}},{"$sort":{"avg_age":-1}},{"$limit":10}]'
m$aggregate(query)

###############################################################################