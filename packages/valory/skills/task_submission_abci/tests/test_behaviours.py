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
from typing import Any, Dict, List, Callable, Optional, Type
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
    expected_log: str
    expected_log_level: int
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

    _GET_LAST_TX_HASH_ERR = "Last tx status is: "

    @pytest.mark.parametrize(
        "test_case",
        [
            BehaviourTestCase(
                name="Last final tx hash is not present",
                initial_data=_INITIAL_DATA,
                ok_reqs=[],
                err_reqs=[],
                expected_log=_GET_LAST_TX_HASH_ERR,
                expected_log_level=logging.INFO,
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
        ) as mock_logger:
            self.behaviour.act_wrapper()

            # apply the OK mocks first
            for ok_req in test_case.ok_reqs:
                ok_req(self)

            # apply the failing mocks
            for err_req in test_case.err_reqs:
                err_req(self, error=True)

            print(f"mock logs: {mock_logger.call_args_list}")

            log_found = False
            for log_args in mock_logger.call_args_list:
                if platform.python_version().startswith("3.7"):
                    actual_log_level, actual_log = log_args[0][:2]
                else:
                    actual_log_level, actual_log = log_args.args[:2]

                if actual_log.startswith(test_case.expected_log):
                    assert actual_log_level == test_case.expected_log_level, (
                        f"{test_case.expected_log} was expected to log on {test_case.expected_log_level} log level, "
                        f"but logged on {log_args[0]} instead."
                    )
                    log_found = True
                    break

            if not log_found:
                raise AssertionError(
                    f'Expected log message "{test_case.expected_log}" was not found in captured logs: '
                    f"{mock_logger.call_args_list}."
                )

        if len(test_case.err_reqs) == 0:
            # no mocked requests fail,
            # the behaviour should complete
            self.complete(test_case.event, test_case.next_behaviour_class)
