"""Data processor module.

This module provides the abstract ``DataProcessor`` base class that defines the contract
for collecting, transforming, and sending intelligence data from external sources.

Pipeline::

    process():
        with self.work_manager:
            self.send(self.transform(self.collect()))

``send()`` handles both cases transparently:

- ``transform()`` returns a ``list`` → sent as a single bundle
- ``transform()`` yields multiple ``list``s → each is sent as a separate bundle

A connector may have multiple ``DataProcessor`` instances for different data types.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Generator
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from connectors_sdk.connectors.external_import.logger import ConnectorLogger
    from connectors_sdk.connectors.external_import.work_manager import WorkManager
    from connectors_sdk.settings.base_settings import BaseConnectorSettings
    from connectors_sdk.state_manager.state_manager import ConnectorStateManager


class DataProcessor(ABC):
    """Abstract base class defining the data processing contract.

    Each ``DataProcessor`` is responsible for:

    - Fetching raw data (``collect()``)
    - Converting it to bundle objects (``transform()``)
    - Sending them to OpenCTI (``send()``)

    ``transform()`` can either return a list (single bundle) or yield
    multiple lists (streaming). ``send()`` detects this and acts accordingly.

    The ``work_manager``, ``logger`` and ``state`` attributes are injected by
    ``BaseExternalImportConnector``.

    The processor can read and write state fields (e.g. cursors, checkpoints)
    via ``self.state``, but it must **not** call ``state.load()`` or
    ``state.save()`` — that is handled by the base connector.

    Attributes:
        work_name: A human-readable name for the work created by this processor.
        config: The connector settings, injected by the base connector.
        work_manager: The ``WorkManager`` instance, injected by the base connector.
        logger: The ``ConnectorLogger`` instance, injected by the base connector.
        state: The ``ConnectorStateManager`` instance, injected by the base connector.
    """

    work_name: str
    config: BaseConnectorSettings
    work_manager: WorkManager
    logger: ConnectorLogger
    state: ConnectorStateManager

    def process(self) -> None:
        """Run the full processing pipeline: collect → transform → send.

        Opens a work via the ``WorkManager`` context manager, then
        composes ``send(transform(collect()))``.

        The work is automatically closed on exit (success, fail, or delete).
        """
        with self.work_manager:
            self.send(self.transform(self.collect()))

    @abstractmethod
    def collect(self) -> Any:
        """Collect raw intelligence from external sources.

        This method should fetch data from the external source (API, file, feed, etc.)
        and return it in its raw form. No STIX conversion should happen here.

        Can return a single value or yield multiple chunks for streaming::

            # All at once
            def collect(self):
                return api_client.get_all_indicators()

            # Streaming
            def collect(self):
                for page in api_client.get_indicators_paginated():
                    yield page

        Returns:
            Raw data from the external source, or a generator yielding chunks.
        """
        ...

    @abstractmethod
    def transform(self, data: Any) -> list[Any] | Generator[list[Any], None, None]:
        """Transform raw data into bundle objects.

        This method should convert the raw data from ``collect()`` into
        STIX 2.1 objects (using stix2 library objects or connectors-sdk model instances).

        Can return a single list or yield multiple lists for streaming::

            # Single bundle
            def transform(self, data):
                return [Indicator(...), Observable(...)]

            # Streaming — data is a generator from collect()
            def transform(self, data):
                for chunk in data:
                    yield [Indicator(...) for item in chunk]

        Args:
            data: The raw data returned (or yielded) by ``collect()``.

        Returns:
            A list of STIX/SDK objects, or a generator yielding lists.
        """
        ...

    def send(
        self, bundle_objects: list[Any] | Generator[list[Any], None, None]
    ) -> None:
        """Send bundle objects to OpenCTI via the work manager.

        If ``bundle_objects`` is a generator, each yielded list is sent
        as a separate bundle. Otherwise the list is sent as a single bundle.

        Args:
            bundle_objects: A list of STIX/SDK objects, or a generator yielding lists.
        """
        if isinstance(bundle_objects, Generator):
            for chunk in bundle_objects:
                if chunk:
                    self.work_manager.send(chunk, self.work_name)
        else:
            if bundle_objects:
                self.work_manager.send(bundle_objects, self.work_name)
