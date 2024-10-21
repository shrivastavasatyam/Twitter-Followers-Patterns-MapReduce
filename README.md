# Twitter Follower's Patterns - MapReduce

This project focuses on analyzing patterns (2-hop paths and Triangles) in a Twitter graph dataset containing nodes and edges using MapReduce and various join techniques. The objective is to implement different MapReduce-based programs to process the Twitter dataset and count 2-hop paths and triangles using Exact, Approximate, RS-Join, and Rep-Join methods. Additionally, we include an edge-counting program and the ability to run all tasks on AWS EMR for scalable distributed processing.

- **2-hop paths**: A 2-hop path exists if there is a path from node X to node Y through an intermediate node Z (i.e., X -> Z -> Y).
- **Triangles**: A triangle exists if three nodes X, Y, and Z are mutually connected (i.e., X -> Y, Y -> Z, Z -> X). These computations provide insights into the structure and connectivity of graphs like the Twitter dataset.

Author
-----------
- Satyam Shrivastava

Installation
------------
This project was set up on an Apple MacBook M1 Pro. The following components need to be installed first:
- OpenJDK 11 (installed via Homebrew)
- Hadoop 3.3.5 (downloaded from https://hadoop.apache.org/release/3.3.5.html)
- Maven (downloaded from https://repo.maven.apache.org/maven2/org/apache/maven/apache-maven/3.6.3/, using the first link)
- AWS CLI 1 (installed via Homebrew)

### Steps to Install

1) Install OpenJDK 11: Install OpenJDK 11 using Homebrew:

   `brew install openjdk@11`

2) Install Hadoop 3.3.5: Download Hadoop 3.3.5, unzip the file, and move it to the appropriate directory:

   `mv hadoop-3.3.5 /usr/local/hadoop-3.3.5`

3) Install Maven 3.6.3: Download Maven 3.6.3, unzip the file, and move it to the appropriate directory:

   `mv apache-maven-3.6.3 /usr/local/apache-maven-3.6.3`

4) Install AWS CLI 1: Install AWS CLI version 1 using Homebrew:

   `brew install awscli@1`

After installation, ensure the components are correctly set up by checking their versions:
```
java -version
hadoop version
mvn -version
aws --version
```

Environment
-----------
1) **Setting up Environment Variables**:

   Environment variables are required to be defined based on the preferred shell. The zsh (Z-Shell) is used for this project on MacOS.

   Following are the relevant environment variables (in `~/.zshrc`) based on project's setup:

   ```
   export JAVA_HOME=/opt/homebrew/opt/openjdk@11

   export HADOOP_HOME=/usr/local/hadoop-3.3.5
   export YARN_CONF_DIR=$HADOOP_HOME/etc/hadoop
   export PATH=$PATH:$HADOOP_HOME/bin:$HADOOP_HOME/sbin

   export M2_HOME=/usr/local/apache-maven-3.6.3
   export PATH=$M2_HOME/bin:$PATH

   export PATH="/opt/homebrew/opt/awscli@1/bin:$PATH"
   ```

2) Explicitly set `JAVA_HOME` in the Hadoop configuration file. Edit `$HADOOP_HOME/etc/hadoop/hadoop-env.sh` and add:

   `export JAVA_HOME=/opt/homebrew/opt/openjdk@11`

Jobs Overview:
-----------
This project involves following MapReduce jobs to process the Twitter dataset:

- **Exact 2-Hop Count** (`exact.Exact2HopCount`): Calculates the exact number of 2-hop paths in the Twitter dataset.
  
- **Approximate 2-Hop Count** (`approx.Approx2HopCount`): Uses an approximation algorithm to estimate the number of 2-hop paths. The `MAX` filter is applied to limit the input size.

- **RS-Join Triangle Count** (`rsjoin.RSJoinTriangleCount`): Computes the triangle count using the RS-Join technique. The `MAX` filter is applied to limit the input size.

- **Rep-Join Triangle Count** (`repjoin.RepJoinTriangleCount`): Computes the triangle count using the Rep-Join technique. The `MAX` filter is applied to limit the input size.

- **Edge Count** (`countedges.CountEdgesAfterMax`): Counts the number of edges after applying a maximum filter value (`MAX`) to limit the dataset size.

Each of these jobs can be run locally or in a distributed environment using AWS EMR.

**Important**: It is required to **manually modify the `job.name` in the Makefile** each time to switch between jobs. This step is required because each job has a different logic, and the correct job class must be specified to run the desired program. For example, use `exact.Exact2HopCount` for the exact 2-hop count job, or `repjoin.RepJoinTriangleCount` for the Rep-Join triangle counting.


Makefile Configuration
---------

The Makefile is pre-configured for different jobs based on the type of operation (Exact 2-Hop Count, Approximate 2-Hop Count, RS-Join, Rep-Join, and Edge Count). To switch between jobs, adjust the following parameters in the Makefile:

```makefile
# Use job names accordingly
# -> Exact: exact.Exact2HopCount
# -> Approx: approx.Approx2HopCount
# -> RSJoin: rsjoin.RSJoinTriangleCount
# -> RepJoin: repjoin.RepJoinTriangleCount
# -> CountEdges: countedges.CountEdgesAfterMax
job.name=repjoin.RepJoinTriangleCount

# Input paths
# -> Use input/edges.csv for Exact, Approx, and CountEdges jobs else use:
local.input=input

# Use output path accordingly
# -> Exact: output/exact
# -> Approx: output/approx
# -> RSJoin: output/rsjoin
# -> RepJoin: output/repjoin
# -> CountEdges: output/countedges
local.output=output/repjoin

# AWS Log paths
# -> RSJoin: awslog/rsjoin
# -> RepJoin: awslog/repjoin
local.awslog=awslog/repjoin
```

Execution
---------
Following are the general steps of execution used for implementing the project:

1) Clone the project's GitHub repository or unzip the project files into Visual Studio Code (or any preferred IDE).

2) Open Terminal window in VS Code. Navigate to directory where project files unzipped.

3) **Edit Makefile** and `pom.xml` **file**:
   All build and execution commands are organized in the Makefile. The Makefile is pre-configured to handle local, pseudo-distributed, and AWS EMR setups. Project can be executed directly using the `make` commands provided below without modifying the Makefile unless we're customizing the environment.
   Additionally, the pom.xml file is configured with the recommended software versions for this course (such as OpenJDK 11, Hadoop 3.3.5, and Maven 3.6.3). Since the same versions are being used as suggested in the course, there's no need to update the pom.xml file
   
   Edit the Makefile to customize the environment at the top. **If necessary, adjust the job names and paths in the Makefile as shown above**.

   Sufficient for standalone: hadoop.root, jar.name, local.input. Other defaults acceptable for running standalone.

5) Standalone Hadoop:
	- `make switch-standalone`		-- set standalone Hadoop environment (execute once)
	- `make local`

6) Test Run with a Small File:
   Before running the full program, it's a good idea to validate the setup with a small test file for visualizing the results easily. Create a simple text file (`sample_edges.txt`) with 8-10 lines in the input folder. Create a separate `outputSample` folder.

```
# sample content
1,2
2,3
3,1
1,4
4,5
5,6
6,4
3,6
2,5
5,3
```
   Create a separate `outputSample` folder. Edit the parameters (local.input and local.output) in Makefile. Execute the respective program locally using the sample file now. Understand the output results.
	- `make local`

8) Pseudo-Distributed Hadoop: (https://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-common/SingleCluster.html#Pseudo-Distributed_Operation)
	- `make switch-pseudo`			-- set pseudo-clustered Hadoop environment (execute once)
	- `make pseudo`					-- first execution
	- `make pseudoq`				-- later executions since namenode and datanode already running

9) AWS EMR Hadoop: (you must configure the emr.* config parameters at top of Makefile)
	- `make make-bucket`			-- Create an S3 bucket (only for the first execution)
	- `make upload-input-aws`		-- Upload input data to AWS S3 (only for the first execution)
	- `make aws`					-- Run the Hadoop job on AWS EMR. Check for successful execution with web interface (aws.amazon.com)
	- `download-output-aws`		-- After successful execution & termination, download the output from S3
 	- `download-log-aws`		-- After successful execution & termination, download the logs from S3

**Important**: Terminate EMR cluster and delete the files from S3 bucket after successful execution of the jobs to avoid unnecessary costs. Monitor EMR usage through AWS to manage expenses effectively.

### Notes: 
- The work was consistently committed (every 15-20 mins) to version control and maintain a proper workflow.
- The input, output, and logs directories (not suited for version control) are added to .gitignore to avoid size issues when uploading to GitHub. These files are stored on OneDrive for easy access without overloading the repository.