# Didomi Challenge - Facundo Martínez
This is the proposed solution for Didomi's Data Engineering challenge. It was built taking into account the 
information contained in the original README.md file. 

## Installation
The solution was created with Click, to facilitate the creation of a command line interface and simplify installation, 
as well as deployment.  

### Minimal installation
You can install the minimum requirements to run the application by simply executing 

```bash
pip install -r requirements.txt
```

This will install the basic requirements for the application to work.

### Development installation
To install it in a development environment, you can use the requirements-dev.txt file by simply running

```bash
pip install -r requirements-dev.txt
```

This will install not only the core dependencies, but also the test and development dependencies used to develop this 
solution.

The creation of a virtualenv is recommended, especially for development purposes, but not mandatory. In a production or
CI environment is not necessary.

## Execution
Once installed, to execute the program you simply need a command line interface. If you type

```bash
python -m didomi --help
```

It should show the help. 

Since this was made with Click, help was automatically generated. Using the `--help` flag on all commands is possible, 
and it should display their respective help alongside all the possible flags and input options.

All the input parameters have defaults that allows the solution to be executed from the project's directory without any
problem. So for instance, if the following command is issued

```bash
python -m didomi normalize
```

It will execute the normalization phase, looking for the input files in the directory `input` and putting the result
in the directory `consent`.

### Configuration file
In order to be able to configure Spark's execution easily, you can pass a .ini file with the expected cluster 
configuration through the command line by simply using the `-sc`/`--spark-config` flag like this:

```bash
python -m didomi -sc my_spark_config.ini normalize
```

This will run task-1 with the expected Spark config. 

The configuration file must contain a section called `[SPARK]`. A template configuration file has been submited with 
this code. Nevertheless, and example of a configuration file could be:

```ini
[SPARK]
spark.app.name=my-app
spark.master=spark://5.6.7.8:7077
spark.executor.memory=4g
spark.eventLog.enabled=true
spark.serializer=org.apache.spark.serializer.KryoSerializer
```

The solution uses Python's `configparser` module to read the configuration. So all the facilities provided by that 
module are applicable.

The motivation behind using a configuration file instead of dynamically set values through the command line, is that you 
can always dynamically create configuration files, and they can be tracked through the various processes that create 
them. Also, they are much easier to put into version control, and to deploy than command line interface flags.

## Testing
I'm using pytest for testing, due to its extensibility and flexibility. Also, it is one of the most maintained testing 
suites in Python. In order to execute the tests, you need to install the development dependencies. To do that, simply 
run the next command:

```bash
pip install -r .\requirements-dev.txt
```

Afterwards, you can execute pytest directly from the project's root directory like this:

```bash
python -m pytest
```

## Considerations and motivations
The following are considerations that were made during the solution's development as well as the motivations for some of 
the solution's characteristics:

### Command Line Interface
It was considered that the main way to execute this solution would be by deploying it into an already existing spark 
cluster or through a platform such as Databricks. Given this, the usage of a command line interface simplifies the 
execution.

### Output file type and fields
The usage of Parquet as the output file type is due to its capabilities to for fast processing and extraction of 
columnar data. Since most of the proposed queries where for summarized KPIs, I believe that doing it with parquet is
one of the bests ways, next to using a Data Warehouse solution.

The fields that are in the output file allows the execution of the queries while maintaining enough information for 
displaying to a non-technical user. Also, the creation of derived fields directly into the file, such as the field 
`token_vendors_enabled`, have both ideas in mind. One, to lessen the burden of later processing in order to 
simplify the engine query plan. And second, to allow a less technical user the ability to filter and group the data 
through the most used fields.

## Possible improvements
One possible improvement is to partition the data through the various fields that are taken into account at the time
of slicing the data. I didn't do it, because further investigation should be applied if partitioning should be made for 
every field that slices the data or just by some. This is because doing lots of partitioning could lead to serious 
performance and costs issues when working with Spark in next steps.