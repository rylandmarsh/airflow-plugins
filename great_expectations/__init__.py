from airflow.plugins_manager import AirflowPlugin
from great_expectations.operators.great_expectations_operator import DatabaseToPandasGreatExpectationsValidationOperator

class GreatExpectationsPlugin(AirflowPlugin):
    name = 'great_expectations_plugin'
    hooks = []
    operators = [DatabaseToPandasGreatExpectationsValidationOperator]
    executors = []
    macros = []
    admin_views = []
    flask_blueprints = []
    menu_links = []
