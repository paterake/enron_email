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
3. Code is executed during development and testing under:

/home/rakesh/Documents/__code/__git/enronEmail


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
1. Take account of the functional-programming conventions of Scala (Java-8) to develop a solution that is minimal in code and maximises code maintainability.
2. Word Count processing is to be performed using RDDs.
3. Recipient Top100 processing is performed using Spark Dataframes, Databricks XML library and Spark SQL.
4. Use Spark SQL to parse the XML to get the required data fields of the #To and #CC fields.


# EC2 Instance Setup
0. Gotcha - ensure EBS volume and subsequent EC2 is created in the same region as the dataset.

1. Create an Amazon EBS volume using the Snapshot ID.

aws ec2 create-volume --snapshot snap-d203feb5 --size 220 --region us-east-1 --availability-zone us-east-1b --volume-type gp2

Make a note of the volume-id

2. From the EC2 console, create a Key-Pair and download.
3. From the EC2 console, create an EC2 instance - in this case a "free-tier" micro-instance.
Create t2.micro EC2 instance on us-east-1b

Make a note of the instance-id

4. Attache the EBS Volume to the EC2 instance

aws ec2 attach-volume --volume-id vol-082bab18bf54a1d1f --instance-id i-090b4458529dcebfc --device /dev/sdf

-- volume-id is captured in point1.
-- instance-id is caputured in point4.

5. Set permissions on the downloaded PEM file
chmod 400 aws_pair_key_1.pem

6. Connect to the EC2 instance:
ssh -i "aws_pair_key_1.pem" ec2-user@ec2-34-227-176-118.compute-1.amazonaws.com

7. Perform updates on the EC2 instance
sudo yum update

8. Mount the ESB Volume to the EC2 instance

sudo file -s /dev/xvdf

sudo mkdir /data

sudo mount /dev/xvdf /data

sudo du -sh /data/*

9. Optional, make the data available via S3 - as a alternate solution
aws configure
AWS Access Key ID [None]: <Access Key ID>
AWS Secret Access Key [None]: <Secret Access Key>

This creates a file under ~/.aws

10. Create S3 Bucket
aws s3api create-bucket --bucket enron-sainsbury --region us-east-1b

11. Copy edrm-enron-v2 (which contains XML data) to S3. edrm-enron-v1 can be ignored
cd /data
aws s3 cp edrm-enron-v2 s3://enron-sainsbury/v2 --recursive --exclude "*" --include "*xml.zip"

# EC2 Instance software setup
1. Connect to EC2 instance

ssh -i "pem-key" ec2-user@ec2-34-227-176-118.compute-1.amazonaws.com

2. Install Java:

sudo yum update

sudo yum install java-1.8.0

sudo yum remove java-1.7.0-openjdk

java -version

	openjdk version "1.8.0_131"

	OpenJDK Runtime Environment (build 1.8.0_131-b11)

	OpenJDK 64-Bit Server VM (build 25.131-b11, mixed mode)


3. Install Apache Spark

wget https://d3kbcqa49mib13.cloudfront.net/spark-2.1.1-bin-hadoop2.7.tgz

sudo tar zxvf spark-2.1.1-bin-hadoop2.7.tgz -C /opt

cd /opt

sudo ln -fs spark-2.1.1-bin-hadoop2.7 /opt/spark

4. Configure

vi .bash_profile

export SPARK_HOME=/opt/spark

PATH=$PATH:$SPARK_HOME/bin

:wq

source ~/.bash_profile

spark-submit --version

5. Update logging level

cp $SPARK_HOME/conf/log4j.properties.template $SPARK_HOME/conf/log4j.properties

vi $SPARK_HOME/conf/log4j.properties

Change

log4j.rootCategory=INFO, console

to

log4j.rootCategory=ERROR, console

6. Test spark

spark-shell

val textFile = spark.sparkContext.textFile("/opt/spark/README.md")

textFile.count()

sys.exit

# EC2 Instance Swap space setup
Assumption is to perform the processing on a micro instance with minimal cost implication.
Memory for a Spark job may be an issue, and thus swap space will be required.

sudo dd if=/dev/zero of=/swapfile bs=1M count=1024

sudo mkswap /swapfile

sudo swapon /swapfile

swapon -s

sudo vi /etc/fstab

Add: /swapfile    none    swap sw 0 0

sudo chown root:root /swapfile

sudo chmod 0600 /swapfile


# Application Install
1. Clone project
git clone https://github.com/paterake/enron_email.git

2. Assemble JAR File
cd enronEmail
sbt assemble

3. Copy Uber JAR to EC2 instance
rsync -azv --progress -e "ssh -i <pem-file>" enronEmail-assembly-1.0.jar  ec2-user@ec2-34-227-176-118.compute-1.amazonaws.com:/home/ec2-user


# Application Execution
Connect to the EC2 instance, and execute the Spark Job:

spark-submit --class com.paterake.enron.SparkEmailParser --master local --driver-memory 1g --executor-memory 1g --executor-cores 1  /home/ec2-user/enronEmail-assembly-1.0.jar /data/edrm-enron-v2/*xml.zip


# Run results
files = 8927

words = 4762419

avg Words Per file = 533

files = 1

Top100 = List((pallen70@hotmail.com,605), (stagecoachmama@hotmail.com,498), (pallen@enron.com,446), (phillip.k.allen@enron.com,405), (jsmith@austintx.com,345), (cbpres@austin.rr.com,222), (mike.grigsby@enron.com,156), (jacquestc@aol.com,149), (jane.m.tholt@enron.com,131), (matt.smith@enron.com,131), (llewter@austin.rr.com,108), (gthorse@keyad.com,108), (keith.holst@enron.com,105), (jay.reitmeyer@enron.com,99), (matthew.lenhart@enron.com,99), (tori.kuykendall@enron.com,97), (randall.l.gay@enron.com,95), (maryrichards7@hotmail.com,94), (steven.p.south@enron.com,91), (frank.ermis@enron.com,89), (susan.m.scott@enron.com,73), (jason.wolfe@enron.com,69), (mark@intelligencepress.com,64), (patti.sullivan@enron.com,63), (ywang@enron.com,60), (stouchstone@natsource.com,60), (mac.d.hargrove@rssmb.com,60), (barry.tycholiz@enron.com,59), (rlehmann@yahoo.com,58), (bs_stone@yahoo.com,56), (mog.heu@enron.com,55), (jason.huang@enron.com,49), (muller@thedoghousemail.com,46), (stephanie.miller@enron.com,44), (thomas.a.martin@enron.com,40), (tim.belden@enron.com,39), (strawbale@crest.org,39), (eric.bass@enron.com,36), (dexter@intelligencepress.com,36), (mark.whitt@enron.com,36), (kim.ward@enron.com,34), (pkaufma@enron.com,34), (bob.m.hall@enron.com,34), (lkuch@mh.com,34), (mike.swerzbin@enron.com,33), (chris.h.foster@enron.com,32), (monique.sanchez@enron.com,32), (matt.motley@enron.com,32), (tom.alonso@enron.com,32), (robert.badeer@enron.com,32), (kam.keiser@enron.com,30), (alb@cpuc.ca.gov,30), (rourke@enron.com,28), (michael.cowan@enron.com,28), (paul.t.lucci@enron.com,28), (gallen@thermon.com,28), (imceanotes-all+20enron+20worldwide+40enron@enron.com,26), (chris.dorland@enron.com,26), (chad.clark@enron.com,26), (hargr@webtv.net,26), (pallen@ect.enron.com,26), (chris.mallory@enron.com,26), (gary@creativepanel.com,24), (jim123@pdq.net,24), (john.arnold@enron.com,24), (kholst@enron.com,24), (djack@keyad.com,22), (hunter.s.shively@enron.com,22), (yevgeny.frolov@enron.com,21), (scott.neal@enron.com,20), (sheri.a.righi@accenture.com,20), (scfatkfa@caprock.net,18), (imceanotes-all+20enron+20houston+40enron@enron.com,18), (donald.l.barnhart@accenture.com,17), (8774820206@pagenetmessage.net,16), (imceanotes-undisclosed-recipients+3a+3b+40enron@enron.com,16), (kolinge@enron.com,16), (wise.counsel@lpl.com,16), (imceanotes-+22jeff+20smith+22+20+3cjsmith+40austintx+2ecom+3e+40enron@enron.com,16), (debe@fsddatasvc.com,16), (moshuffle@hotmail.com,16), (christopher.f.calger@enron.com,16), (john.zufferli@enron.com,16), (retwell@sanmarcos.net,16), (david.l.johnson@enron.com,16), (jedglick@hotmail.com,16), (brendas@tgn.net,16), (brendas@surffree.com,16), (scsatkfa@caprock.net,16), (zimam@enron.com,16), (kevin.m.presto@enron.com,16), (mery.l.brown@accenture.com,15), (daniel.lisk@enron.com,15), (johnniebrown@juno.com,14), (lodonnell@spbank.com,14), (approval.eol.gas.traders@enron.com,14), (jeff.richter@enron.com,14), (chris.gaskill@enron.com,14), (western.price.survey.contacts@ren-6.cais.net,14), (paul.kaufman@enron.com,14))


