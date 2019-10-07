import logging
from typing import Union, Mapping, Iterable

from airflow.exceptions import AirflowException
from airflow.hooks.base_hook import BaseHook
from airflow.models import BaseOperator
from airflow.plugins_manager import AirflowPlugin
from airflow.utils.decorators import apply_defaults

import great_expectations as ge

log = logging.getLogger(__name__)

class DatabaseToPandasGreatExpectationsValidationOperator(BaseOperator):
    """
    Copies data based on SQL provided from a source DB location to a pandas data frame.
    Runs a great expectations validation config on this file as specified and raises
    a airflow exception if it fails.
    :param conn_id: Database connection id
    :type conn_id: str
    :param sql: sql string to use, can also be a sql file (.sql)
    :type sql: str
    :param validation_config: serialised dictionary of ge validations or the path to a file (.json)
    :type validation_config: str
    :param parameters: dictionary of params
    :type parameters: mapping or iterable
    """

    template_fields = ('validation_config', 'sql')
    template_ext = ('.json', '.sql')

    @apply_defaults
    def __init__(self,
                 conn_id=None,
                 sql=None,
                 validation_config=None,
                 parameters: Union[Mapping, Iterable] = None,
                 *args,
                 **kwargs):
        self.conn_id = conn_id
        self.sql = sql
        self.parameters = parameters
        self.validation_config = validation_config
        super(DatabaseToPandasGreatExpectationsValidationOperator, self).__init__(*args, **kwargs)

    def execute(self, context):

        if self.sql is None:
            raise AirflowException("sql must be specified")

        if self.validation_config is None or len(self.validation_config) == 0:
            raise AirflowException(
                "validation config is not there or it's empty")

        db = BaseHook.get_hook(self.conn_id)

        df = ge.from_pandas(db.get_pandas_df(sql=self.sql, parameters=self.parameters))
        df.expectations_config = self.validation_config

        self.log.info("Validating Table Contents")
        validation_object = df.validate()

        self.log.info(validation_object)
        if not validation_object['success']:
            raise AirflowException(
                "Validation failed, there's something wrong with the dataframe object, check the validation output")

        self.log.info("Validation successful")
