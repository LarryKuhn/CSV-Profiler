# CSV Profiler Script as an AWS Lambda Function

These sections describe how to implement csvprofiler as an AWS Lambda function using the Python code provided in this folder, using Amazon EFS or S3 as the input and output storage medium. In both cases, CSV Profiler has no knowledge of being run as a Lambda function since the code provided acts as a shell and calls `csvprofiler.py` from a Lambda layer.

* __<span>lambda_function-efs.py</span>__ - provides example code that can be used to process CSV files stored on an EFS file system.

* __<span>lambda_function-S3.py</span>__ - provides example code that can be used to process CSV files stored in S3.

## Creating the Lambda Layer

Create the Lambda Layer for use in either implementation (EFS or S3), as follows:

1. Log in to AWS CloudShell or an EC2 Amazon Linux instance (the layer must be created in the same OS as the Lambda runs in).

2. Run the following commands to create the python.zip Lambda Layer file and upload into an existing S3 bucket:

    ```bash
    mkdir python
    python3 -m pip install --target ./python numpy
    python3 -m pip install --target ./python pandas
    python3 -m pip install --target ./python csvprofiler
    zip -r9 python.zip ./python
    aws s3 cp ./python.zip s3://somebucket/somefolder/python.zip
    # clean up
    rm ./python -r
    rm ./python.zip
    ```

3. Create a new lambda layer referencing the zip file saved in S3 above -- give it a name and specify Python 3.8 compatibility.

## Configuring CSV Profiler as a Lambda function using EFS

### Configuring Amazon EFS

1. This documentation assumes that you have an EFS file system all ready setup.  If not, creating one is pretty easy to do following AWS documentation.

2. Once the EFS File System has been created, create an EFS Access Point so Lambda has a way to connect to the file system.

3. Copying files into the file system will likely require that you have an EC2 instance running with the EFS file system mounted on it.  Setting up folders for running the profiler software would then pretty much be the same as running it on single machine (e.g. laptop or server) and could be done without Lambda.  The simple Lambda script was created as an exercise but could be an option for running routine validations without relying on an EC2 instance.

4. The Lambda script has some code commented out that can be used to copy files from S3 into EFS for testing, which would allow you to bypass needing to use an EC2 instance, although working with EFS without an EC2 instance can be cumbersome.

5. The Lambda script also has code commented out that can be used to copy output files from EFS into S3 when the profiling software has finished processing as an alternative method for reviewing the output.

### Configuring Amazon VPC and S3

1. To use EFS in a Lambda function, the Lambda needs to run in a VPC and private subnet where the EFS file system is available.

    * Once the VPC, subnet and security group are available, setup the Lambda as this may save you from having to perform some of the changes below if Lambda does them for you.

2. A security group needs to be configured for the subnet allowing traffic to the EFS file system and, optionally, S3.

3. For EFS, allow inbound and outbound traffic on port 2049, outbound with a destination of the same security group (it points to itself).

4. If using S3, create an S3 VPC end point allowing traffic to S3 from the VPC. Ensure the security group allows traffic to the S3 prefix list (e.g. pl-63a5400a for com.amazonaws.us-east-1.s3).

### Configuring the AWS Lambda to use EFS

1. Create the Lambda Layer as described above prior to configuration.

2. Create a new Lambda function "from scratch" with Python 3.8 as the runtime and let the console create a new execution role. Go into `Advanced Settings` and select the VPC, subnet and security group to use.

3. In Code Source, paste the code from `lambda_function-efs.py` into `lambda_function.py`.  Remove comments from S3 code if desired.

4. Add the Lambda Layer created above.

5. Select the Configuration Tab

    * General Configuration - Ensure it is configured with an appropriate amount of memory and execution time.

    * VPC - Check that the VPC, subnet, security group with inbound and outbound rules required are defined.

    * File System - Select your EFS File System and Access Point. Create a mount point as `/mnt/efs`.

    * Permissions - Log-group permissions should already exist. The other permissions may as well. Permissions needed are the 3 CloudWatch log actions from AWSLambdaBasicExecutionRole, AWSLambdaVPCAccessRole providing 3 permissions for Network Interface management and additional EC2 permissions for describing VPC, subnet and security group resources (see below), and any additional S3 permissions if needed. Check the execution role itself in IAM, Lambda should be a "trusted" entity.

        ```bash
        Allow: ec2:DescribeVpcs
        Allow: ec2:DescribeSubnets
        Allow: ec2:DescribeSecurityGroups
        Allow: ec2:CreateNetworkInterface
        Allow: ec2:DeleteNetworkInterface
        Allow: ec2:DescribeNetworkInterfaces
        ```

6. Config file formatting should be the same as running on a local machine, except using `/mnt/efs` in the file paths. For example:

    ```bash
    [Paths]
    file_path      = /mnt/efs/csvprofiler
    csv_file       = %(file_path)s/input/input.csv
    param_file     = %(file_path)s/params/params.csv
    report_file    = %(file_path)s/output/report.txt

    error_path     = %(file_path)s
    error_csv_file = %(error_path)s/output/errors.csv
    error_log_file = %(error_path)s/output/errors.log
    ```

7. Place any requisite config, csv and parameter files in the appropriate EFS folders.

8. To test, use any input file (e.g. helloworld default in Lambda); the `event` is ignored in the Lambda. This of course would be changed to something like using an API Gateway trigger, perhaps with the config file attached or a file pointer in production use.

## Configuring CSV Profiler as a Lambda function using S3

An important note to keep in mind is that the approach described below copies the CSV file from S3 to temporary storage where the Lambda function is executing. This means that all input and output files must fit into the storage provided by AWS Lambda, which as of this writing is approximately 512MB. Streaming the input CSV file is technically possible to avoid this limitation but would likely require changes to the internals of `csvprofiler.py`.

### Configuring Amazon S3

1. Create an S3 bucket for storing input and output files.

2. Create folders for config, input, output and params (or in whatever fashion you wish, matching the paths in your config file).

3. Note that AWS recommends NOT storing trigger files in the same bucket as other working files to avoid runaway processes, so create a different bucket for the config folder if that makes you more comfortable.  If you do so, be aware that the script pulls the bucket name from the `event` object and uses it for all input and output, so the Lambda code would need to be adjusted accordingly.

4. You should be able to run the Lambda from within a VPC, but you may need to setup an S3 access point if you do.

### Configuring the AWS Lambda to use S3

1. Create the Lambda Layer as described above prior to configuration.

2. Create a new Lambda function "from scratch" with Python 3.8 as the runtime and let the console create a new execution role.

3. In Code Source, paste the code from `lambda_function-S3.py` into `lambda_function.py`.

4. Add the Lambda Layer created above.

5. Add the Config File Trigger

    * Choose the S3 service and bucket name created above.

    * Select Event Type `ObjectCreatedByPut`, provide the prefix (folder name) `config/` and suffix `.cfg`.

6. Select the Configuration Tab

    * General Configuration - Ensure it is configured with an appropriate amount of memory and execution time.

    * Permissions - Add S3 bucket permissions to the execution role (for both buckets if 2 were created); log-group permissions should already exist.  There should also be a Resource-Based Policy allowing S3 to trigger the Lambda function.

    * Environment Variables - If you don't like how the values are coded in the Lambda function, these can be used for added flexibility.

7. Execution is driven by uploading a config file to the `config` folder of the S3 bucket. The script obtains the bucket name and key from the `event` object in the `lambda_handler` to get the config file location, but environment variables could be used to set other input/output buckets and locations. Within the config file, the file pointers in the `[PATH]` section need to be formatted a certain way for the Lambda to function properly, as below. Note that the folders/prefixes are NOT preceeded by a slash "/".  If they are, the script will fail.

    ```bash
    [Paths]
    file_path      = 
    csv_file       = input/input.csv
    param_file     = params/params.csv
    report_file    = output/report.txt

    error_path     = 
    error_csv_file = output/errors.csv
    error_log_file = output/errors.log
    ```

8. Place any requisite test and parameter files in the appropriate S3 buckets and folders.

9. Copy a config file to the `config` folder to test the lambda (e.g. `aws s3 cp ./input.cfg s3://somebucket/config/input.cfg`). You should see results in both the Lambda service and in CloudWatch.

## Licensing

Copyright (c) 2021 Larry Kuhn (larrykuhn at outlook.com)

This program is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation, either version 3 of the License, or
any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program.  If not, see <https://www.gnu.org/licenses/>.
A copy can be found with the project at:
    https://github.com/LarryKuhn/CSV-Profiler/blob/master/LICENSE
