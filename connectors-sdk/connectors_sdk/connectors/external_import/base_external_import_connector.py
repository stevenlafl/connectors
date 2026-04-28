"""Base external import connector module.

This module provides the ``BaseExternalImportConnector`` class that serves as the foundation
for all external import connectors. It handles the common orchestration logic:
state management, scheduling, error handling, and running data processors.

Architecture::

    BaseExternalImportConnector
    ├── OpenCTIConnectorHelper → pycti bridge (created once)
    ├── ConnectorLogger        → Logging (wraps helper's AppLogger)
    ├── ConnectorStateManager  → State persistence (last_run, custom fields)
    └── DataProcessor[]        → process(): with work_manager: send(transform(collect()))
        └── WorkManager        → context manager: open work → send → close work
"""

import sys
from datetime import datetime, timezone

from connectors_sdk.connectors.external_import.data_processor import DataProcessor
from connectors_sdk.connectors.external_import.logger import ConnectorLogger
from connectors_sdk.connectors.external_import.work_manager import WorkManager
from connectors_sdk.settings.base_settings import BaseConnectorSettings
from connectors_sdk.state_manager.state_manager import ConnectorStateManager
from pycti import OpenCTIConnectorHelper


class BaseExternalImportConnector:
    """Base class for external import connectors.

    This class provides the common orchestration logic for external import connectors:

    - State management (``last_run`` tracking via ``ConnectorStateManager``)
    - Scheduling (periodic execution via ``schedule_process``)
    - Error handling and logging
    - Running one or more ``DataProcessor`` instances

    The ``OpenCTIConnectorHelper`` is created once from the settings and shared
    across all internal components. Connector code never needs to import from
    ``pycti`` directly.

    A connector may have **multiple processors** to handle different data types
    (e.g. one for indicators, one for reports, one for vulnerabilities).

    Attributes:
        config: The connector configuration (subclass of ``BaseConnectorSettings``).
        logger: The ``ConnectorLogger`` for logging without direct pycti dependency.
        state: The ``ConnectorStateManager`` for persisting connector state.
        data_processors: The list of ``DataProcessor`` instances.

    Example:
        >>> class IndicatorProcessor(DataProcessor):
        ...     work_name = "Indicators import"
        ...     def collect(self):
        ...         return api_client.get_indicators()
        ...     def transform(self, data):
        ...         return [to_stix(d) for d in data]
        ...     # send() inherited — handles list or generator from transform()
        ...
        >>> settings = MyConnectorSettings()
        >>> connector = BaseExternalImportConnector(
        ...     config=settings,
        ...     data_processors=[IndicatorProcessor()],
        ... )
        >>> connector.run()
    """

    def __init__(
        self,
        config: BaseConnectorSettings,
        data_processors: list[DataProcessor],
        state: ConnectorStateManager | None = None,
    ) -> None:
        """Initialize the base external import connector.

        Args:
            config: The connector configuration settings.
            data_processors: The list of ``DataProcessor`` instances to run.
            state: Optional custom state manager. If ``None``, the default
                ``ConnectorStateManager`` is used.
        """
        if not data_processors:
            raise ValueError("At least one DataProcessor must be provided.")
        self.config = config
        self._helper = OpenCTIConnectorHelper(config=config.to_helper_config())
        self.logger = ConnectorLogger(self._helper)
        self.state = state if state is not None else ConnectorStateManager(self._helper)
        self.data_processors = data_processors
        for processor in self.data_processors:
            processor.config = config
            processor.work_manager = WorkManager(
                self._helper, self.logger, processor.work_name
            )
            processor.logger = self.logger
            processor.state = self.state

    def callback(self) -> None:
        """Main processing method for the connector.

        This method orchestrates the full processing pipeline:

        1. Load the connector state from OpenCTI
        2. Run each ``DataProcessor`` inside its ``WorkManager`` context
        3. Update state with ``last_run``

        Override this method for fully custom processing logic.
        """
        connector_name = self.config.connector.name
        self.logger.info(
            "[CONNECTOR] Starting connector...",
            {"connector_name": connector_name},
        )

        try:
            self.state.load(force=True)

            if self.state.last_run:
                self.logger.info(
                    "[CONNECTOR] Connector last run",
                    {"last_run_datetime": str(self.state.last_run)},
                )
            else:
                self.logger.info("[CONNECTOR] Connector has never run...")

            self.logger.info(
                "[CONNECTOR] Running connector...",
                {"connector_name": connector_name},
            )

            for processor in self.data_processors:
                processor.process()

            self.state.last_run = datetime.now(tz=timezone.utc)
            self.state.save()

            self.logger.info(
                f"{connector_name} connector successfully run, "
                f"storing last_run as {self.state.last_run}"
            )

        except (KeyboardInterrupt, SystemExit):
            self.logger.info(
                "[CONNECTOR] Connector stopped...",
                {"connector_name": connector_name},
            )
            sys.exit(0)
        except Exception as err:
            self.logger.error(str(err))

    def run(self) -> None:
        """Start the connector with scheduled execution.

        Uses ``OpenCTIConnectorHelper.schedule_process`` to run ``callback``
        at the interval defined by ``connector.duration_period`` in the configuration.

        The scheduler also checks the connector's queue size. If ``CONNECTOR_QUEUE_THRESHOLD``
        is set and the queue exceeds the threshold, the main process will be paused
        until the queue is reduced.

        Note:
            The ``config.connector`` must be a ``BaseExternalImportConnectorConfig``
            (or subclass) with a ``duration_period`` field.
        """
        self._helper.schedule_process(
            message_callback=self.callback,
            duration_period=self.config.connector.duration_period.total_seconds(),  # type: ignore[attr-defined]
        )
