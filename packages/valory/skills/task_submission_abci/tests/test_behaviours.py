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
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Generator, Callable, Optional, Type
from unittest import mock

import pytest

# from packages.valory.contracts.agent_mech.contract import (
#     AgentMechContract,
# )
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
    FinishedWithoutTasksRound,
)
from packages.valory.contracts.gnosis_safe.contract import GnosisSafeContract
from packages.valory.protocols.contract_api import ContractApiMessage
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


SAFE_CONTRACT_ADDRESS = "0x8969Bd87b9e743d8120e41445462F0cBE29f5D7C"
# MECH_ADDRESS = "0x77af31De935740567Cf4fF1986D04B2c964A786a"
MULTISEND_ADDRESS = "0xA238CBeb142c10Ef7Ad8442C6D1f9E89e07e7761"


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

        with mock.patch.object(
            self.behaviour.current_behaviour.context.logger, "log"
        ) as mock_logger, self.dummy_get_done_tasks_wrapper(
            tasks=test_case.initial_data["done_tasks"]
        ), self.dummy_remove_tasks_wrapper():
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
