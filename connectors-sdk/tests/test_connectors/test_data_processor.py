# pragma: no cover
# type: ignore
from collections.abc import Generator
from typing import Any
from unittest.mock import MagicMock

from connectors_sdk.connectors.external_import.data_processor import DataProcessor
from connectors_sdk.connectors.external_import.logger import ConnectorLogger
from connectors_sdk.connectors.external_import.work_manager import WorkManager


class ListProcessor(DataProcessor):
    """Processor that returns a list from transform."""

    work_name = "List Import"

    def collect(self) -> list[str]:
        return ["raw1", "raw2"]

    def transform(self, data: Any) -> list[Any]:
        return [f"stix-{d}" for d in data]


class GeneratorProcessor(DataProcessor):
    """Processor that yields chunks from transform."""

    work_name = "Generator Import"

    def collect(self) -> list[str]:
        return ["a", "b", "c"]

    def transform(self, data: Any) -> Generator[list[Any], None, None]:
        for item in data:
            yield [f"stix-{item}"]


class EmptyListProcessor(DataProcessor):
    """Processor that returns an empty list from transform."""

    work_name = "Empty Import"

    def collect(self) -> list:
        return []

    def transform(self, data: Any) -> list[Any]:
        return []


class EmptyChunkGeneratorProcessor(DataProcessor):
    """Processor that yields some empty chunks."""

    work_name = "Mixed Import"

    def collect(self) -> list:
        return ["a", "", "b"]

    def transform(self, data: Any) -> Generator[list[Any], None, None]:
        for item in data:
            if item:
                yield [f"stix-{item}"]
            else:
                yield []


class TestDataProcessor:
    def _inject_deps(
        self,
        processor: DataProcessor,
        mock_helper: MagicMock,
        mock_logger: ConnectorLogger,
    ) -> None:
        processor.config = MagicMock()
        processor.work_manager = WorkManager(
            mock_helper, mock_logger, processor.work_name
        )
        processor.logger = mock_logger
        processor.state = MagicMock()

    def test_process_list(self, mock_helper: MagicMock, mock_logger: ConnectorLogger):
        proc = ListProcessor()
        self._inject_deps(proc, mock_helper, mock_logger)
        proc.process()
        mock_helper.api.work.initiate_work.assert_called_once()
        mock_helper.send_stix2_bundle.assert_called_once()

    def test_process_generator(
        self, mock_helper: MagicMock, mock_logger: ConnectorLogger
    ):
        proc = GeneratorProcessor()
        self._inject_deps(proc, mock_helper, mock_logger)
        proc.process()
        mock_helper.api.work.initiate_work.assert_called_once()
        # 3 chunks → 3 send calls
        assert mock_helper.send_stix2_bundle.call_count == 3

    def test_process_empty_list(
        self, mock_helper: MagicMock, mock_logger: ConnectorLogger
    ):
        proc = EmptyListProcessor()
        self._inject_deps(proc, mock_helper, mock_logger)
        proc.process()
        # Empty list → no work created
        mock_helper.api.work.initiate_work.assert_not_called()

    def test_process_generator_skips_empty_chunks(
        self, mock_helper: MagicMock, mock_logger: ConnectorLogger
    ):
        proc = EmptyChunkGeneratorProcessor()
        self._inject_deps(proc, mock_helper, mock_logger)
        proc.process()
        # Only 2 non-empty chunks sent
        assert mock_helper.send_stix2_bundle.call_count == 2

    def test_send_passes_work_name(
        self, mock_helper: MagicMock, mock_logger: ConnectorLogger
    ):
        proc = ListProcessor()
        self._inject_deps(proc, mock_helper, mock_logger)
        # Call send directly to verify work_name is passed
        with proc.work_manager:
            proc.send(["obj1", "obj2"])
        mock_helper.api.work.initiate_work.assert_called_once_with(
            "test-connector-id", "List Import"
        )
