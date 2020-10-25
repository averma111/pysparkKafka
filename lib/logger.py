class Log4j:
    def __init__(self, spark):
        log4j = spark._jvm.org.apache.log4j
    # Getting package name from log4j.properties
        root_class = 'ashish.learning.pyspark.projects'
    # Creating the conf for spark
        conf = spark.sparkContext.getConf()
        app_id = conf.get('spark.app.id')
        app_name = conf.get('spark.app.name')
        message_prefix = '<' + app_name + ' ' + app_id + '>'
        self.logger = log4j.LogManager.getLogger(root_class + "." + message_prefix)

    def error(self, message):
        """Log an error.
        :param: Error message to write to log
        :return: None
        """
        self.logger.error(message)
        return None

    def warn(self, message):
        """Log an warning.
        :param: Error message to write to log
        :return: None
        """
        self.logger.warn(message)
        return None

    def info(self, message):
        """Log information.
        :param: Information message to write to log
        :return: None
        """
        self.logger.info(message)
        return None
