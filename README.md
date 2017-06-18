# Enron Email Parsing

Processing of the Enron Email Data using Apache Spark.
Data is available as a public dataset via AWS.

# Requirements
1. Determine the average size of emails in terms of words.
2. Determine the Top 100 email recipients.  
2.1. Recipients in the "To" field are weighted at 100%
2.2. Recipients in the "CC" field are weighted at 50%.

# Assumptions - Global
1. Since only Version 2 of the data uses XML, the processing is against the data in the folder edrm-enron-v2.
2. Only the ZIP files for files suffixed "_xml.zip" will be considered for processing.

# Assumptions - Word Count
1. Of these ZIP files, only those files with an extension of ".txt" will be considered as files representing email content.  
2. This implies only files under the folder "text_000" will be processed - although this is not a consideration in the code developed.
3. As stated in point 4 all files in the ZIP with an extension of ".txt" will be considered as email files.
4. Words are defined as a group of characters seperated by white space in the ".txt" file.
5. In word count processing the entirety of the file is considered, no accounting for the structure of file.
6. No support provided for de-duplication of the email sent to multiple mailboxes (this can be considered a future enhancement).

# Assumptions - Recipient Top 100
1. Recipient processing is performed against the XML files in the ZIP file.
2. Recipients in the "To" field are weighted with a value of 2.
3. Recipients in the "CC" field are weighted with a value of 1.
4. Recipients are defined as follows:
4.1. Normal email addresses are recognised as characters surrounding an ampersand.
4.2. LDAP addresses are recognised words after "CN=" ending with ">"

# Non Functional Requirements.
1. Minimise any form of data movement.  Moving data is costly in terms of time and resources.
2. Mount public data set to an EC2 instance and process directly on the EC2 instance.
3. Code developed will minimise resource usage.  ZIP files will be processed in-memory (RDDs and Dataframes).
4. Temp space on disk will NOT be used for processing contents of ZIP files.
5. Final application processing will be performed in the EC2 instance using the lowest cost options available.
6. Work of the principle that code moves to where the data exists.

7. In contradiction to NFR point 1, copy required data from the edrm-enron-v2 folder to S3.
8. Having data available in S3 is as a backup to enable processing of the final spark job via EMR, as a fall-back solution.
9. Points 7 and 8 are fallback, since this adds to the total number of moving parts in the final solution.

10. Package final solution as an Uber JAR, but ensure minimal JAR dependencies limited to the inclusion of just the Databricks XML library.


# Techniques.
1. Word Count processing is to be performed using RDDs.
2. Recipient Top100 processing is performed using Spark Dataframes, Databricks XML library and Spark SQL.
3. Take account of the functional-programming conventions of Scala (Java-8) to develop a solution that is minimal in code and maximises code maintainability.


# EC2 Instance Setup
1. Create an Amazon EBS volume using the Snapshot ID.
aws ec2 create-volume --snapshot snap-d203feb5 --size 220 --region us-east-1 --availability-zone us-east-1b --volume-type gp2

Gotcha - ensure EBS volume is created in the same region as the dataset.

2. From the EC2 console, create a Key-Pair and download.
3. From the EC2 console
