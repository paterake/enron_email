# enron_email
Enron Email Parser

Processing Enron Email Data using Apache Spark.  
Data is available as a public dataset via AWS.

# Requirements
1. Determine the average size of emails in terms of words.
2. Determine the Top 100 email recipients.  
2.1. Recipients in the "To" field are weighted at 100%
2.2. Recipients in the "CC" field are weighted at 50%.

# Assumptions - global
1. Since only Version 2 of the data uses XML, the processing is against the data in the folder edrm-enron-v2.
2. Only the ZIP files for files suffixed "_xml.zip" will be considered for processing.

# Assumptions - word count
1. Of these ZIP files, only those files with an extension of ".txt" will be considered as files representing email content.  
2. This implies only files under the folder "text_000" will be processed - although this is not a consideration in the code developed.
3. As stated in point 4 all files in the ZIP with an extension of ".txt" will be considered as email files.
4. Words are defined as a group of characters seperated by white space in the ".txt" file.
5. In word count processing the entirety of the file is considered, no accounting for the structure of file.
6. No support provided for de-duplication of the email sent to multiple mailboxes (this can be considered a future enhancement).

# Assumptions - recipients
1. Recipient processing is performed against the XML files in the ZIP file.
2. Recipients in the "To" field are weighted with a value of 2.
3. Recipients in the "CC" field are weighted with a value of 1.
4. Recipients are defined as follows:
4.1. Normal email addresses are recognised as characters surrounding an ampersand.
4.2. LDAP addresses are recognised words after "CN=" ending with ">"

Non Functional Requirements.
1. 
