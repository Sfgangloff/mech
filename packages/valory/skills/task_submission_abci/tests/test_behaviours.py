# -*- coding: utf-8 -*-
# ------------------------------------------------------------------------------
#
#   Copyright 2022-2023 Valory AG
#
#   Licensed under the Apache License, Version 2.0 (the "License");
#   you may not use this file except in compliance with the License.
#   You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#   Unless required by applicable law or agreed to in writing, software
#   distributed under the License is distributed on an "AS IS" BASIS,
#   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#   See the License for the specific language governing permissions and
#   limitations under the License.
#
# ------------------------------------------------------------------------------

"""Test the behaviours.py module of the skill."""
import logging
import platform
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, cast, Generator, Callable, Optional, Type
from unittest import mock

import pytest

from packages.valory.contracts.hash_checkpoint.contract import HashCheckpointContract
from packages.valory.contracts.agent_registry.contract import AgentRegistryContract
from packages.valory.contracts.agent_mech.contract import AgentMechContract
from packages.valory.contracts.service_registry.contract import ServiceRegistryContract

# from packages.valory.contracts.multisend.contract import (
#     MultiSendContract,
# )
from packages.valory.skills.task_submission_abci.behaviours import (
    TaskExecutionBaseBehaviour,
    TaskPoolingBehaviour,
    TransactionPreparationBehaviour,
)
from packages.valory.skills.task_submission_abci.rounds import (
    Event,
    SynchronizedData,
    FinishedTaskPoolingRound,
    FinishedWithoutTasksRound,
    FinishedTaskExecutionWithErrorRound,
)
from packages.valory.contracts.gnosis_safe.contract import GnosisSafeContract
from packages.valory.protocols.contract_api import ContractApiMessage
from packages.valory.protocols.ledger_api import LedgerApiMessage
from packages.valory.protocols.contract_api.custom_types import RawTransaction, State
from packages.valory.skills.abstract_round_abci.base import AbciAppDB
from packages.valory.skills.abstract_round_abci.behaviours import (
    BaseBehaviour,
    make_degenerate_behaviour,
)
from packages.valory.skills.abstract_round_abci.test_tools.base import (
    FSMBehaviourBaseCase,
)
from packages.valory.skills.task_submission_abci import PUBLIC_ID


SAFE_CONTRACT_ADDRESS = "0x5e1D1eb61E1164D5a50b28C575dA73A29595dFf7"
HASH_CHECKPOINT_ADDRESS = "0x694e62BDF7Ff510A4EE66662cf4866A961a31653"
MECH_ADDRESS = "0x77af31De935740567Cf4fF1986D04B2c964A786a"
SERVICE_REGISTRY_ADDRESS = "0x9338b5153AE39BB89f50468E608eD9d764B755fD"
AGENT_REGISTRY_ADDRESS = "0xE49CB081e8d96920C38aA7AB90cb0294ab4Bc8EA"
MULTISEND_ADDRESS = "0xA238CBeb142c10Ef7Ad8442C6D1f9E89e07e7761"
ZERO_IPFS_HASH = (
    "f017012200000000000000000000000000000000000000000000000000000000000000000"
)


def test_skill_public_id() -> None:
    """Test skill module public ID"""

    # pylint: disable=no-member
    assert PUBLIC_ID.name == Path(__file__).parents[1].name
    assert PUBLIC_ID.author == Path(__file__).parents[3].name


@dataclass
class BehaviourTestCase:
    """BehaviourTestCase"""

    name: str
    initial_data: Dict[str, Any]
    ok_reqs: List[Callable]
    err_reqs: List[Callable]
    expected_logs: List[str]
    expected_log_levels: List[int]
    event: Event = Event.DONE
    next_behaviour_class: Optional[Type[BaseBehaviour]] = None


class BaseTaskSubmissionTest(FSMBehaviourBaseCase):
    """Base test case."""

    path_to_skill = Path(__file__).parent.parent

    behaviour: TaskExecutionBaseBehaviour  # type: ignore
    behaviour_class: Type[BaseBehaviour]
    next_behaviour_class: Type[BaseBehaviour]
    synchronized_data: SynchronizedData
    done_event = Event.DONE

    def fast_forward(self, data: Optional[Dict[str, Any]] = None) -> None:
        """Fast-forward on initialization"""

        data = data if data is not None else {}
        self.fast_forward_to_behaviour(
            self.behaviour,  # type: ignore
            self.behaviour_class.auto_behaviour_id(),
            SynchronizedData(AbciAppDB(setup_data=AbciAppDB.data_to_lists(data))),
        )
        assert (
            self.behaviour.current_behaviour is not None
            and self.behaviour.current_behaviour.behaviour_id
            == self.behaviour_class.auto_behaviour_id()
        )

    def complete(
        self, event: Event, next_behaviour_class: Optional[Type[BaseBehaviour]] = None
    ) -> None:
        """Complete test"""
        if next_behaviour_class is None:
            # use the class value as fallback
            next_behaviour_class = self.next_behaviour_class

        self.behaviour.act_wrapper()
        self.mock_a2a_transaction()
        self._test_done_flag_set()
        self.end_round(done_event=event)
        assert (
            self.behaviour.current_behaviour is not None
            and self.behaviour.current_behaviour.behaviour_id
            == next_behaviour_class.auto_behaviour_id()
        )


class TestTaskPoolingBehaviour(BaseTaskSubmissionTest):
    """Tests TaskPoolingBehaviour"""

    behaviour_class = TaskPoolingBehaviour

    _SAFE_OWNERS = ["0x1", "0x2", "0x3", "0x4"]
    _NUM_SAFE_OWNERS = len(_SAFE_OWNERS)
    _SAFE_THRESHOLD = 1
    _MOCK_TX_RESPONSE = b"0xIrrelevantForTests".hex()
    _MOCK_TX_HASH = "0x" + "0" * 64
    _INITIAL_DATA: Dict[str, Any] = dict(
        all_participants=_SAFE_OWNERS,
        safe_contract_address=SAFE_CONTRACT_ADDRESS,
        participants=_SAFE_OWNERS,
        consensus_threshold=3,
    )
    _DUMMY_DONE_TASKS = [
        {
            "request_id": 58758105316598692706519173691512343367950130276409284865927422102201674760225,
            "mech_address": "0x262FF09Bf2496fa62264bA71dFBE9b85794193A7",
            "task_executor_address": "0x264968d975cAA715e1D65c2b5902fE9ea64E6F28",
            "tool": "prediction-online",
            "request_id_nonce": "None",
            "is_marketplace_mech": False,
            "task_result": "43d21586462cbd9ce086ca7d0c5559316378d08c60599f71b48b7cf2dd3fd063",
        },
        {
            "request_id": 85336656703727246191869830228415812710148856408201745592663531215206922045768,
            "mech_address": "0x262FF09Bf2496fa62264bA71dFBE9b85794193A7",
            "task_executor_address": "0x264968d975cAA715e1D65c2b5902fE9ea64E6F28",
            "tool": "prediction-online",
            "request_id_nonce": None,
            "is_marketplace_mech": False,
            "task_result": "b711c818be38a1a4803bc43c29e44b70ea02b0888ed553b5fed7111cd27e6684",
        },
        {
            "request_id": 100045748490114249017905977519000015185197459576092770991271139356043347620871,
            "mech_address": "0x262FF09Bf2496fa62264bA71dFBE9b85794193A7",
            "task_executor_address": "0x264968d975cAA715e1D65c2b5902fE9ea64E6F28",
            "tool": None,
            "request_id_nonce": None,
            "is_marketplace_mech": False,
            "task_result": "c84c814458b4af5d98fea6d9b19aede3ea370b49b62ad94cdc8b9fa60e9058e3",
        },
    ]

    _GET_LAST_TX_HASH_INFO = "Last tx status is: {status}"
    _GET_TASK_REMOVE_INFO = "Tasks [] has already been submitted. Removing them from the list of tasks to be processed."

    def dummy_get_done_tasks_wrapper(
        self,
        tasks: List[dict],
    ) -> mock._patch:
        """Mock BaseBehaviour.get_done_tasks method."""

        def dummy_get_done_tasks(
            *_: Any, **__: Any
        ) -> Generator[None, None, Optional[List[dict]]]:
            """Dummy `get_done_tasks` method."""
            yield
            return tasks

        return mock.patch.object(
            self.behaviour.current_behaviour,
            "get_done_tasks",
            side_effect=dummy_get_done_tasks,
        )

    def dummy_remove_tasks_wrapper(
        self,
    ) -> mock._patch:
        """Mock BaseBehaviour.remove_tasks method."""

        def dummy_remove_tasks(
            *_: Any, **__: Any
        ) -> Generator[None, None, Optional[List[dict]]]:
            """Dummy `remove_tasks` method."""
            yield
            return []

        return mock.patch.object(
            self.behaviour.current_behaviour,
            "remove_tasks",
            side_effect=dummy_remove_tasks,
        )

    @pytest.mark.parametrize(
        "test_case",
        [
            BehaviourTestCase(
                name="Last final tx hash is not present",
                initial_data=_INITIAL_DATA | {"done_tasks": []},
                ok_reqs=[],
                err_reqs=[],
                expected_logs=[_GET_LAST_TX_HASH_INFO.format(status=False)],
                expected_log_levels=[logging.INFO],
                event=Event.NO_TASKS,
                next_behaviour_class=make_degenerate_behaviour(
                    FinishedWithoutTasksRound
                ),
            ),
            BehaviourTestCase(
                name="Last final tx hash is None",
                initial_data=_INITIAL_DATA | {"done_tasks": []},
                ok_reqs=[],
                err_reqs=[],
                expected_logs=[_GET_LAST_TX_HASH_INFO.format(status=False)],
                expected_log_levels=[logging.INFO],
                event=Event.NO_TASKS,
                next_behaviour_class=make_degenerate_behaviour(
                    FinishedWithoutTasksRound
                ),
            ),
            BehaviourTestCase(
                name="Last final tx hash is present",
                initial_data=_INITIAL_DATA
                | {"done_tasks": [], "final_tx_hash": _MOCK_TX_HASH},
                ok_reqs=[],
                err_reqs=[],
                expected_logs=[
                    _GET_LAST_TX_HASH_INFO.format(status=True),
                    _GET_TASK_REMOVE_INFO,
                ],
                expected_log_levels=[logging.INFO, logging.INFO],
                event=Event.NO_TASKS,
                next_behaviour_class=make_degenerate_behaviour(
                    FinishedWithoutTasksRound
                ),
            ),
            BehaviourTestCase(
                name="Behaviour ends with done tasks",
                initial_data=_INITIAL_DATA
                | {"done_tasks": _DUMMY_DONE_TASKS, "final_tx_hash": _MOCK_TX_HASH},
                ok_reqs=[],
                err_reqs=[],
                expected_logs=[
                    _GET_LAST_TX_HASH_INFO.format(status=True),
                ],
                expected_log_levels=[logging.INFO],
                event=Event.DONE,
                next_behaviour_class=TransactionPreparationBehaviour,
            ),
        ],
    )
    def test_run(self, test_case: BehaviourTestCase) -> None:
        """Test multiple paths"""
        self.fast_forward(data=test_case.initial_data)
        # repeating this check for the `current_behaviour` here to avoid `mypy` reporting:
        # `error: Item "None" of "Optional[BaseBehaviour]" has no attribute "context"` when accessing the context below
        assert self.behaviour.current_behaviour is not None

        with (
            mock.patch.object(
                self.behaviour.current_behaviour.context.logger, "log"
            ) as mock_logger,
            self.dummy_get_done_tasks_wrapper(
                tasks=test_case.initial_data["done_tasks"]
            ),
            self.dummy_remove_tasks_wrapper(),
        ):
            self.behaviour.act_wrapper()

            # apply the OK mocks first
            for ok_req in test_case.ok_reqs:
                ok_req(self)

            # apply the failing mocks
            for err_req in test_case.err_reqs:
                err_req(self, error=True)

            assert len(test_case.expected_logs) == len(test_case.expected_log_levels)

            for log, log_level in zip(
                test_case.expected_logs, test_case.expected_log_levels
            ):

                log_found = False
                for log_args in mock_logger.call_args_list:
                    if platform.python_version().startswith("3.7"):
                        actual_log_level, actual_log = log_args[0][:2]
                    else:
                        actual_log_level, actual_log = log_args.args[:2]

                    if str(actual_log).startswith(log):
                        assert actual_log_level == log_level, (
                            f"{log} was expected to log on {log_level} log level, "
                            f"but logged on {log_args[0]} instead."
                        )
                        log_found = True
                        break

                if not log_found:
                    raise AssertionError(
                        f'Expected log message "{log}" was not found in captured logs: '
                        f"{mock_logger.call_args_list}."
                    )

        if len(test_case.err_reqs) == 0:
            # no mocked requests fail,
            # the behaviour should complete
            self.complete(test_case.event, test_case.next_behaviour_class)


class TestTransactionPreparationBehaviour(BaseTaskSubmissionTest):
    """Tests TransactionPreparationBehaviour"""

    behaviour_class = TransactionPreparationBehaviour

    _SAFE_OWNERS = ["0x1", "0x2", "0x3", "0x4"]
    _AGENTS = ["0x5", "0x6", "0x7", "0x8"]
    _NUM_SAFE_OWNERS = len(_SAFE_OWNERS)
    _SAFE_THRESHOLD = 1
    _INITIAL_DATA: Dict[str, Any] = dict(
        all_participants=_SAFE_OWNERS,
        safe_contract_address=SAFE_CONTRACT_ADDRESS,
        participants=_SAFE_OWNERS,
        consensus_threshold=3,
    )
    _MOCK_TX_RESPONSE = b"0xIrrelevantForTests".hex()
    _MOCK_TX_HASH = "0x" + "0" * 64
    _MOCK_ENCODE_DATA = "0x" + "0" * 64
    _MOCK_CHECKPOINT_DATA = cast(
        bytes, "0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef"
    )
    _MOCK_BALANCE = 1e18
    _MOCK_SERVICE_OWNER = "0x15bd56669F57192a97dF41A2aa8f4403e9491776"
    _MOCK_OPERATORS_MAPPING = dict(zip(_SAFE_OWNERS, _AGENTS))
    _MOCK_TOKEN_HASH = cast(
        bytes, "0x714ef7dafa358c7152f6703dd764a1df40d369dcf53275dd2543b0fdbf207298"
    )
    _MOCK_IPFS_HASH = (
        "f0170122043807431fe61981e7177a204e872a55069f4c9d9bfb118f1096ee79025c223ed"
    )
    _MOCK_IPFS_DATA = dict(
        zip(
            _AGENTS,
            [
                dict({"tool_1": 10, "tool_2": 4, "tool_3": 1, "tool_4": 1111}),
                dict({"tool_1": 1, "tool_2": 9, "tool_3": 10, "tool_4": 43}),
                dict({"tool_1": 5, "tool_2": 15, "tool_3": 2, "tool_4": 55}),
                dict({"tool_1": 8, "tool_2": 2, "tool_3": 6, "tool_4": 38}),
            ],
        )
    )
    _DUMMY_DONE_TASKS = [
        {
            "request_id": 1,
            "task_executor_address": _AGENTS[0],
            "tool": "tool_1",
            "mech_address": MECH_ADDRESS,
            "is_marketplace_mech": False,
            "task_result": "result_1",
            "request_id_nonce": 1,
        },
        {
            "request_id": 2,
            "task_executor_address": _AGENTS[1],
            "tool": "tool_2",
            "mech_address": MECH_ADDRESS,
            "is_marketplace_mech": False,
            "task_result": "result_2",
            "request_id_nonce": 1,
        },
        {
            "request_id": 3,
            "task_executor_address": _AGENTS[2],
            "tool": "tool_3",
            "mech_address": MECH_ADDRESS,
            "is_marketplace_mech": False,
            "task_result": "result_3",
            "request_id_nonce": 1,
        },
        {
            "request_id": 4,
            "task_executor_address": _AGENTS[3],
            "tool": "tool_4",
            "mech_address": MECH_ADDRESS,
            "is_marketplace_mech": False,
            "task_result": "result_4",
            "request_id_nonce": 1,
        },
    ]

    @property
    def state(self) -> TransactionPreparationBehaviour:
        """Current behavioural state"""
        return cast(TransactionPreparationBehaviour, self.behaviour.current_behaviour)

    @property
    def mocked_metadata_hash(self) -> mock._patch_dict:
        """Mocked configured metadata hash address"""
        return mock.patch.dict(
            self.state.params.__dict__,
            {"metadata_hash": self._MOCK_IPFS_HASH},
        )

    @property
    def mocked_agent_registry_address(self) -> mock._patch_dict:
        """Mocked agent registry address"""
        return mock.patch.dict(
            self.state.params.__dict__,
            {"agent_registry_address": AGENT_REGISTRY_ADDRESS},
        )

    @property
    def mocked_service_registry_address(self) -> mock._patch_dict:
        """Mocked service registry address"""
        return mock.patch.dict(
            self.state.params.__dict__,
            {"service_registry_address": SERVICE_REGISTRY_ADDRESS},
        )

    @property
    def mocked_hash_checkpoint_address(self) -> mock._patch_dict:
        """Mocked hash checkpoint address"""
        return mock.patch.dict(
            self.state.params.__dict__,
            {"hash_checkpoint_address": HASH_CHECKPOINT_ADDRESS},
        )

    def mocked_profit_split_freq(self, value) -> mock._patch_dict:
        """Mocked profit_split_freq param"""
        return mock.patch.dict(
            self.state.params.__dict__,
            {"profit_split_freq": value},
        )

    def regex_search(self, expected_log, actual_log):
        if re.search(expected_log, actual_log):
            return True
        else:
            return False

    _GET_CONTRACT_REQUEST_ERR = "{func_name} unsuccessful!:"
    _SHOULD_UPDATE_HASH_WARN = "Could not get latest hash. Don't update the metadata."
    _DELIVERY_REPORT_WARN = "Could not get current usage."
    _NUM_REQS_AGENT_WARN = "Could not get delivery report. Don't split profits."
    _NUM_REQS_AGENT_DELIVERED_WARN = (
        "Could not get number of requests delivered. Don't split profits."
    )
    _SHOULD_NOT_SPLIT_PROFITS_INFO = "Not splitting profits."
    _IPFS_USAGE_DATA_WARN = "Could not get usage data from IPFS:"
    _SHOULD_SPLIT_PROFITS_INFO = "Splitting profits"
    _GET_BALANCE_ERR = (
        r"Could not get profits from mech 0x[a-fA-F0-9]{40}\. Don't split profits."
    )
    _GET_BALANCE_SUCCESS = (
        r"Got (-?\d+(\.\d+)?|(-?\d+e[+-]?\d+)) profits from mech 0x[a-fA-F0-9]{40}"
    )
    _GET_SERVICE_OWNER_WARN = "Could not get service owner. Don't split profits."
    _SPLIT_FUNDS_ERR = (
        r"Could not split profits from mech 0x[a-fA-F0-9]{40}\. Don't split profits."
    )

    def wrap_dummy_get_from_ipfs(self, data: Optional[Any]) -> Callable:
        """Wrap dummy_get_from_ipfs."""

        def dummy_get_from_ipfs(
            *args: Any, **kwargs: Any
        ) -> Generator[None, None, Optional[Any]]:
            """A mock get_from_ipfs."""
            return data
            yield

        return dummy_get_from_ipfs

    # DeliverBehaviour mocks
    def _mock_get_latest_hash_contract_request(
        self,
        error: bool = False,
    ) -> None:
        """Mock the HashCheckpointContract.get_latest_hash"""

        if not error:
            response_performative = ContractApiMessage.Performative.STATE
            response_body = dict(data=self._MOCK_IPFS_HASH)
        else:
            response_performative = ContractApiMessage.Performative.ERROR
            response_body = dict()

        self.mock_contract_api_request(
            contract_id=str(HashCheckpointContract.contract_id),
            request_kwargs=dict(
                performative=ContractApiMessage.Performative.GET_STATE,
                contract_address=HASH_CHECKPOINT_ADDRESS,
            ),
            response_kwargs=dict(
                performative=response_performative,
                state=State(
                    ledger_id="ethereum",
                    body=response_body,
                ),
            ),
        )

    # FundsSplittingBehaviour mocks
    def _mock_get_balance_ledger_request(
        self,
        error: bool = False,
    ) -> None:
        """Mock the get_balance"""

        if not error:
            response_performative = LedgerApiMessage.Performative.STATE
            response_body = dict(get_balance_result=self._MOCK_BALANCE)
        else:
            response_performative = LedgerApiMessage.Performative.ERROR
            response_body = dict()

        self.mock_ledger_api_request(
            request_kwargs=dict(
                performative=LedgerApiMessage.Performative.GET_STATE,
            ),
            response_kwargs=dict(
                performative=response_performative,
                state=State(
                    ledger_id="ethereum",
                    body=response_body,
                ),
            ),
        )

    def _mock_get_exec_tx_data_contract_request(
        self,
        error: bool = False,
    ) -> None:
        """Mock the AgentMechContract.get_exec_tx_data"""

        if not error:
            response_performative = ContractApiMessage.Performative.STATE
            response_body = dict(data=self._MOCK_TX_HASH)
        else:
            response_performative = ContractApiMessage.Performative.ERROR
            response_body = dict()

        self.mock_contract_api_request(
            contract_id=str(AgentMechContract.contract_id),
            request_kwargs=dict(
                performative=ContractApiMessage.Performative.GET_STATE,
                contract_address=MECH_ADDRESS,
            ),
            response_kwargs=dict(
                performative=response_performative,
                state=State(
                    ledger_id="ethereum",
                    body=response_body,
                ),
            ),
        )

    def _mock_get_service_owner_contract_request(
        self,
        error: bool = False,
    ) -> None:
        """Mock the ServiceRegistryContract.get_service_owner"""

        if not error:
            response_performative = ContractApiMessage.Performative.STATE
            response_body = dict(data=self._MOCK_SERVICE_OWNER)
        else:
            response_performative = ContractApiMessage.Performative.ERROR
            response_body = dict()

        self.mock_contract_api_request(
            contract_id=str(ServiceRegistryContract.contract_id),
            request_kwargs=dict(
                performative=ContractApiMessage.Performative.GET_STATE,
                contract_address=SERVICE_REGISTRY_ADDRESS,
            ),
            response_kwargs=dict(
                performative=response_performative,
                state=State(
                    ledger_id="ethereum",
                    body=response_body,
                ),
            ),
        )

    def _mock_get_operators_mapping_contract_request(
        self,
        error: bool = False,
    ) -> None:
        """Mock the ServiceRegistryContract.get_operators_mapping"""

        if not error:
            response_performative = ContractApiMessage.Performative.STATE
            response_body = dict(data=self._MOCK_OPERATORS_MAPPING)
        else:
            response_performative = ContractApiMessage.Performative.ERROR
            response_body = dict()

        self.mock_contract_api_request(
            contract_id=str(ServiceRegistryContract.contract_id),
            request_kwargs=dict(
                performative=ContractApiMessage.Performative.GET_STATE,
                contract_address=SERVICE_REGISTRY_ADDRESS,
            ),
            response_kwargs=dict(
                performative=response_performative,
                state=State(
                    ledger_id="ethereum",
                    body=response_body,
                ),
            ),
        )

    # TrackingBehaviour mocks
    def _mock_get_checkpoint_data_contract_request(
        self,
        error: bool = False,
    ) -> None:
        """Mock the HashCheckpointContract.get_checkpoint_data"""

        if not error:
            response_performative = ContractApiMessage.Performative.STATE
            response_body = dict(data=self._MOCK_CHECKPOINT_DATA)
        else:
            response_performative = ContractApiMessage.Performative.ERROR
            response_body = dict()

        self.mock_contract_api_request(
            contract_id=str(HashCheckpointContract.contract_id),
            request_kwargs=dict(
                performative=ContractApiMessage.Performative.GET_STATE,
                contract_address=HASH_CHECKPOINT_ADDRESS,
            ),
            response_kwargs=dict(
                performative=response_performative,
                state=State(
                    ledger_id="ethereum",
                    body=response_body,
                ),
            ),
        )

    # HashUpdateBehaviour mocks
    def _mock_get_token_hash_contract_request(
        self,
        error: bool = False,
    ) -> None:
        """Mock the AgentRegistryContract.get_token_hash"""

        if not error:
            response_performative = ContractApiMessage.Performative.STATE
            response_body = dict(data=self._MOCK_TOKEN_HASH)
        else:
            response_performative = ContractApiMessage.Performative.ERROR
            response_body = dict()

        self.mock_contract_api_request(
            contract_id=str(AgentRegistryContract.contract_id),
            request_kwargs=dict(
                performative=ContractApiMessage.Performative.GET_STATE,
                contract_address=AGENT_REGISTRY_ADDRESS,
            ),
            response_kwargs=dict(
                performative=response_performative,
                state=State(
                    ledger_id="ethereum",
                    body=response_body,
                ),
            ),
        )

    def _mock_get_update_hash_tx_data_contract_request(
        self,
        error: bool = False,
    ) -> None:
        """Mock the AgentRegistryContract.get_update_hash_tx_data"""

        if not error:
            response_performative = ContractApiMessage.Performative.STATE
            response_body = dict(data=self._MOCK_ENCODE_DATA)
        else:
            response_performative = ContractApiMessage.Performative.ERROR
            response_body = dict()

        self.mock_contract_api_request(
            contract_id=str(AgentRegistryContract.contract_id),
            request_kwargs=dict(
                performative=ContractApiMessage.Performative.GET_STATE,
                contract_address=AGENT_REGISTRY_ADDRESS,
            ),
            response_kwargs=dict(
                performative=response_performative,
                state=State(
                    ledger_id="ethereum",
                    body=response_body,
                ),
            ),
        )

    # TransactionPreparationBehaviour mocks
    def _mock_get_deliver_data_contract_request(
        self,
        error: bool = False,
    ) -> None:
        """Mock the AgentMechContract.get_deliver_data"""

        if not error:
            response_performative = ContractApiMessage.Performative.STATE
            response_body = dict(data=self._MOCK_TX_RESPONSE, simulation_ok=True)
        else:
            response_performative = ContractApiMessage.Performative.ERROR
            response_body = dict()

        self.mock_contract_api_request(
            contract_id=str(AgentMechContract.contract_id),
            request_kwargs=dict(
                performative=ContractApiMessage.Performative.GET_STATE,
                contract_address=MECH_ADDRESS,
            ),
            response_kwargs=dict(
                performative=response_performative,
                state=State(
                    ledger_id="ethereum",
                    body=response_body,
                ),
            ),
        )

    test_cases = [
        BehaviourTestCase(
            name="Get Update Tx hash fails",
            initial_data=_INITIAL_DATA,
            ok_reqs=[],
            err_reqs=[_mock_get_token_hash_contract_request],
            expected_logs=[
                _GET_CONTRACT_REQUEST_ERR.format(func_name="get_token_hash"),
                _SHOULD_UPDATE_HASH_WARN,
            ],
            expected_log_levels=[logging.WARNING, logging.WARNING],
            event=Event.DONE,
            next_behaviour_class=make_degenerate_behaviour(FinishedTaskPoolingRound),
        ),
        BehaviourTestCase(
            name="Get Update Tx hash success and Get Update hash tx data fails",
            initial_data=_INITIAL_DATA,
            ok_reqs=[_mock_get_token_hash_contract_request],
            err_reqs=[_mock_get_update_hash_tx_data_contract_request],
            expected_logs=[
                _GET_CONTRACT_REQUEST_ERR.format(func_name="get_mech_update_hash")
            ],
            expected_log_levels=[logging.WARNING],
            event=Event.DONE,
            next_behaviour_class=make_degenerate_behaviour(FinishedTaskPoolingRound),
        ),
        BehaviourTestCase(
            name="get_mech_update_hash_tx success, get_latest_hash fails",
            initial_data=_INITIAL_DATA,
            ok_reqs=[
                _mock_get_token_hash_contract_request,
                _mock_get_update_hash_tx_data_contract_request,
            ],
            err_reqs=[_mock_get_latest_hash_contract_request],
            expected_logs=[
                _GET_CONTRACT_REQUEST_ERR.format(func_name="get_latest_hash"),
                _DELIVERY_REPORT_WARN,
                _NUM_REQS_AGENT_WARN,
                _NUM_REQS_AGENT_DELIVERED_WARN,
                _SHOULD_NOT_SPLIT_PROFITS_INFO,
            ],
            expected_log_levels=[
                logging.WARNING,
                logging.WARNING,
                logging.WARNING,
                logging.WARNING,
                logging.INFO,
            ],
            event=Event.DONE,
            next_behaviour_class=make_degenerate_behaviour(FinishedTaskPoolingRound),
        ),
        BehaviourTestCase(
            name="get_mech_update_hash_tx success, get_latest_hash success, ipfs fails",
            initial_data=_INITIAL_DATA
            | {
                "done_tasks": _DUMMY_DONE_TASKS,
            },
            ok_reqs=[
                _mock_get_token_hash_contract_request,
                _mock_get_update_hash_tx_data_contract_request,
                _mock_get_latest_hash_contract_request,
            ],
            err_reqs=[_mock_get_deliver_data_contract_request],
            expected_logs=[
                _IPFS_USAGE_DATA_WARN,
                _DELIVERY_REPORT_WARN,
                _NUM_REQS_AGENT_WARN,
                _NUM_REQS_AGENT_DELIVERED_WARN,
                _SHOULD_NOT_SPLIT_PROFITS_INFO,
                _GET_CONTRACT_REQUEST_ERR.format(func_name="get_deliver_data"),
            ],
            expected_log_levels=[
                logging.WARNING,
                logging.WARNING,
                logging.WARNING,
                logging.WARNING,
                logging.INFO,
                logging.WARNING,
            ],
            event=Event.ERROR,
            next_behaviour_class=make_degenerate_behaviour(
                FinishedTaskExecutionWithErrorRound
            ),
        ),
        BehaviourTestCase(
            name="get_mech_update_hash_tx success, get_latest_hash success, ipfs success",
            initial_data=_INITIAL_DATA
            | {"done_tasks": _DUMMY_DONE_TASKS, "ipfs_response": _MOCK_IPFS_DATA},
            ok_reqs=[
                _mock_get_token_hash_contract_request,
                _mock_get_update_hash_tx_data_contract_request,
                _mock_get_latest_hash_contract_request,
            ],
            err_reqs=[_mock_get_deliver_data_contract_request],
            expected_logs=[],
            expected_log_levels=[],
            event=Event.ERROR,
            next_behaviour_class=make_degenerate_behaviour(
                FinishedTaskExecutionWithErrorRound
            ),
        ),
        BehaviourTestCase(
            name="get_mech_update_hash_tx success, _should_split_profits success, balance fails",
            initial_data=_INITIAL_DATA
            | {
                "done_tasks": _DUMMY_DONE_TASKS,
                "ipfs_response": _MOCK_IPFS_DATA,
                "profit_split_freq": 1,
            },
            ok_reqs=[
                _mock_get_token_hash_contract_request,
                _mock_get_update_hash_tx_data_contract_request,
                _mock_get_latest_hash_contract_request,
            ],
            err_reqs=[_mock_get_balance_ledger_request],
            expected_logs=[_SHOULD_SPLIT_PROFITS_INFO, _GET_BALANCE_ERR],
            expected_log_levels=[logging.INFO, logging.ERROR],
            event=Event.DONE,
            next_behaviour_class=make_degenerate_behaviour(FinishedTaskPoolingRound),
        ),
        BehaviourTestCase(
            name="get_mech_update_hash_tx success, _should_split_profits success, balance success, service owner fails",
            initial_data=_INITIAL_DATA
            | {
                "done_tasks": _DUMMY_DONE_TASKS,
                "ipfs_response": _MOCK_IPFS_DATA,
                "profit_split_freq": 1,
            },
            ok_reqs=[
                _mock_get_token_hash_contract_request,
                _mock_get_update_hash_tx_data_contract_request,
                _mock_get_latest_hash_contract_request,
                _mock_get_balance_ledger_request,
            ],
            err_reqs=[_mock_get_service_owner_contract_request],
            expected_logs=[
                _SHOULD_SPLIT_PROFITS_INFO,
                _GET_BALANCE_SUCCESS,
                _GET_CONTRACT_REQUEST_ERR.format(func_name="get_service_owner"),
                _GET_SERVICE_OWNER_WARN,
                _SPLIT_FUNDS_ERR,
            ],
            expected_log_levels=[
                logging.INFO,
                logging.INFO,
                logging.WARNING,
                logging.WARNING,
                logging.ERROR,
            ],
            event=Event.DONE,
            next_behaviour_class=make_degenerate_behaviour(FinishedTaskPoolingRound),
        ),
    ]

    @pytest.mark.parametrize(
        "test_case",
        test_cases,
        ids=[test_case.name for test_case in test_cases],
    )
    def test_run(self, test_case: BehaviourTestCase) -> None:
        """Test multiple paths"""
        self.fast_forward(data=test_case.initial_data)
        # repeating this check for the `current_behaviour` here to avoid `mypy` reporting:
        # `error: Item "None" of "Optional[BaseBehaviour]" has no attribute "context"` when accessing the context below
        assert self.behaviour.current_behaviour is not None

        with (
            mock.patch.object(
                self.behaviour.current_behaviour.context.logger, "log"
            ) as mock_logger,
            self.mocked_metadata_hash,
            self.mocked_agent_registry_address,
            self.mocked_service_registry_address,
            self.mocked_hash_checkpoint_address,
            mock.patch.object(
                BaseBehaviour,
                "get_from_ipfs",
                side_effect=self.wrap_dummy_get_from_ipfs(
                    test_case.initial_data.get("ipfs_response", None)
                ),
            ),
            self.mocked_profit_split_freq(
                value=test_case.initial_data.get("profit_split_freq", 1000)
            ),
        ):
            self.behaviour.act_wrapper()

            # apply the OK mocks first
            for ok_req in test_case.ok_reqs:
                ok_req(self)

            # apply the failing mocks
            for err_req in test_case.err_reqs:
                err_req(self, error=True)

            assert len(test_case.expected_logs) == len(test_case.expected_log_levels)

            for log, log_level in zip(
                test_case.expected_logs, test_case.expected_log_levels
            ):

                log_found = False
                for log_args in mock_logger.call_args_list:
                    if platform.python_version().startswith("3.7"):
                        actual_log_level, actual_log = log_args[0][:2]
                    else:
                        actual_log_level, actual_log = log_args.args[:2]

                    if str(actual_log).startswith(log) or self.regex_search(
                        log, actual_log
                    ):
                        assert actual_log_level == log_level, (
                            f"{log} was expected to log on {log_level} log level, "
                            f"but logged on {log_args[0]} instead."
                        )
                        log_found = True
                        break

                if not log_found:
                    raise AssertionError(
                        f'Expected log message "{log}" was not found in captured logs: '
                        f"{mock_logger.call_args_list}."
                    )

        if len(test_case.err_reqs) == 0:
            # no mocked requests fail,
            # the behaviour should complete
            self.complete(test_case.event, test_case.next_behaviour_class)
