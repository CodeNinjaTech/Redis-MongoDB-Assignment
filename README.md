# Redis & MongoDB Assignment

## Instructions

You are going to use REDIS and MongoDB to perform an analysis on data related to classified ads from the used motorcycles market.

1. Install REDIS and MongoDB on your workstations. Version 4 of REDIS for Windows is available [here](https://github.com/tporadowski/redis/releases). If you have an older version, make sure that you upgrade since some of the commands needed for the assignment are not supported by older versions. The installation process is straightforward.
2. Download the BIKES_DATASET.zip dataset from [here](https://drive.google.com/open?id=1m4W6anTDphWRnHDwsh-hlexOGrAkMrSq).
3. Download the RECORDED_ACTIONS.zip dataset from [here](https://drive.google.com/open?id=1wyL8nQKDEu6rdr9BH6CgBwGnPnvRT8cJ).
4. Do the tasks listed in the “TASKS” section.

## Scenario

You are a data analyst at a consulting firm and you have access to a dataset of ~30K classified ads from the used motorcycles market. You also have access to some seller related actions that have been tracked in the previous months. You are asked to create a number of programs/queries for the tasks listed in the “TASKS” section.

## Assignment Notes

- You may work on any programming language of your choice. Code samples are provided in R but the choice of language is up to you.
- Working with R is recommended, since the material uploaded on Moodle uses R in order to demonstrate Redis’ usage.
- Assignment should be done in groups of two.
- The dataset is in JSON format. It needs cleaning. You don’t need to follow the guidelines provided below. You may do the cleaning any way you like.
- In your deliverable, you should include (along with your code) a report justifying the steps you took in order to perform the tasks. The report should be VERY brief.
- ONE deliverable per team. The names of the members of each team along with their AM should be included in the first page of the report.
- Your code should be fully commented.
- Your deliverable should be a .zip file named as AM1_AM2.zip
- Optional tasks will have no effect on your final grade. However it’s strongly recommended that you at least try these out in order to understand the actual benefit of the tools/technologies that you are using.
- You don’t have to follow the tips provided in the tasks. You can do it any way you prefer. However, they may come in handy.
- The bitmaps.r file contains code samples for working with bitmaps and REDIS through R.
- The mongo.r file contains code samples for working with MongoDB and JSON files through R.

## Tasks

### Task 1

In this task you are going to use the “recorded actions” dataset in order to generate some analytics with REDIS.

At the end of each month, the classifieds provider sends a personalized e-mail to some of the sellers with a number of suggestions on how they could improve their listings. Some e-mails may have been sent two or three times in the same month due to a technical issue. Not all users open these e-mails. However, we keep track of the e-mails that have been read by their recipients. Apart from that you are also given access to a dataset containing all the user ids along with a flag on whether they performed at least one modification on their listing for each month.

In brief, the datasets are the following:
-	emails_sent.csv “Sets of EmailID, UserID, MonthID and EmailOpened”
-	modified_listings.csv “Sets of UserID, MonthID, ModifiedListing”

The first dataset contains User IDs that have received an e-mail at least once. The second dataset contains all the User IDs of the classifieds provider and a flag that indicates whether the user performed a modification on his/her listing. Both datasets contain entries for the months January, February and March.

You are asked to answer a number of questions using REDIS Bitmaps. A Bitmap is the data structure that immediately pops in your head when the need is to map Boolean information for a huge domain into a compact representation. REDIS, being an in-memory data structure server, provides support for bit manipulation operations. However, there isn’t a special data structure for Bitmaps in REDIS. Rather, bit level operations are supported on the basic REDIS structure: Strings. Now, the maximum length for REDIS strings is 512 MB. Thus, the largest domain that REDIS can map as a Bitmap is 2^32 (512 MB = 2^29 bytes = 2^32 bits).

Bitmaps examples:

Let’s take the following bitmap as an example. Each bit corresponds to a client. Our company has 8 clients in total. The value of 1 means that the client purchased something from our online store in August:

AugustSales:

| 0 | 1 | 2 | 3 | 4 | 5 | 6 | 7 |
|---|---|---|---|---|---|---|---|
| 0 | 1 | 1 | 0 | 1 | 0 | 0 | 0 |

| 0 | 1 | 1 | 0 | 1 | 0 | 0 | 0 |

![Screenshot 2024-03-24 220017](https://github.com/CodeNinjaTech/Redis-MongoDB-Assignment/assets/143879796/63bf5017-94fc-4399-90d5-71b1d17e03bc)

-	Clients at the 0,3,5,6,7 positions did not purchase anything. 
-	Clients at the 1,2,4 positions did at least one transaction in August.

Let’s add another bitmap to the example. It contains the September sales of the same company for the exact same clients: 

SeptemberSales:



### Task 2

In this task you are going to use the “bikes” dataset in order to generate some analytics with MongoDB.

2.1 Add your data to MongoDB.

...

