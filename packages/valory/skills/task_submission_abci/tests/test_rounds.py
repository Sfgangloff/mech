# -*- coding: utf-8 -*-
# ------------------------------------------------------------------------------
#
#   Copyright 2021-2023 Valory AG
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

"""Test the rounds.py module of the skill."""

import json
from copy import deepcopy
from typing import cast
from unittest.mock import MagicMock

import pytest

from packages.valory.skills.abstract_round_abci.base import (
    ABCIAppInternalError,
    TransactionNotValidError,
)
from packages.valory.skills.abstract_round_abci.test_tools.rounds import (
    BaseRoundTestClass,
)
from packages.valory.skills.task_submission_abci.payloads import (
    TaskPoolingPayload,
    TransactionPayload,
)
from packages.valory.skills.task_submission_abci.rounds import (
    Event,
    SynchronizedData,
    TaskPoolingRound,
    TransactionPreparationRound,
)


class TestTaskPoolingRound(BaseRoundTestClass):
    """Tests for TaskPoolingRound."""

    _synchronized_data_class = SynchronizedData
    _event_class = Event
    _payload_data_str = json.dumps(
        [
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
    )

    def test_run_done(
        self,
    ) -> None:
        """Run tests."""

        test_round = TaskPoolingRound(
            synchronized_data=deepcopy(self.synchronized_data),
            context=MagicMock(
                params=MagicMock(
                    default_chain_id=1,
                )
            ),
        )

        first_payload, *payloads = [
            TaskPoolingPayload(sender=participant, content=self._payload_data_str)
            for participant in self.participants
        ]

        test_round.process_payload(first_payload)
        assert test_round.collection == {first_payload.sender: first_payload}
        assert test_round.end_block() is None

        for payload in payloads:
            test_round.process_payload(payload)

        payload_data = json.loads(self._payload_data_str)
        expected_state = self.synchronized_data.update(done_tasks=payload_data)

        res = test_round.end_block()
        assert res is not None
        actual_state, event = res

        assert (
            cast(SynchronizedData, actual_state).done_tasks
            == cast(  # pylint: disable=no-member
                SynchronizedData, expected_state
            ).done_tasks
        )

        assert event == Event.DONE

    def test_run_no_tasks(
        self,
    ) -> None:
        """Run tests."""

        test_round = TaskPoolingRound(
            synchronized_data=deepcopy(self.synchronized_data),
            context=MagicMock(
                params=MagicMock(
                    default_chain_id=1,
                )
            ),
        )
        payload_data = json.dumps([])
        first_payload, *payloads = [
            TaskPoolingPayload(sender=participant, content=payload_data)
            for participant in self.participants
        ]

        test_round.process_payload(first_payload)
        assert test_round.collection == {first_payload.sender: first_payload}
        assert test_round.end_block() is None

        for payload in payloads:
            test_round.process_payload(payload)

        res = test_round.end_block()
        assert res is not None
        actual_state, event = res

        assert actual_state.done_tasks == []

        assert event == Event.NO_TASKS

    def test_bad_payload(
        self,
    ) -> None:
        """Run tests."""

        test_round = TaskPoolingRound(
            synchronized_data=deepcopy(self.synchronized_data),
            context=MagicMock(),
        )

        bad_participant = "non_existent"
        bad_participant_payload = TaskPoolingPayload(
            sender=bad_participant, content=self._payload_data_str
        )
        first_payload, *_ = [
            TaskPoolingPayload(sender=participant, content=self._payload_data_str)
            for participant in self.participants
        ]

        with pytest.raises(
            TransactionNotValidError,
            match=f"{bad_participant} not in list of participants",
        ):
            test_round.check_payload(bad_participant_payload)

        with pytest.raises(
            ABCIAppInternalError,
            match=f"{bad_participant} not in list of participants",
        ):
            test_round.process_payload(bad_participant_payload)

        # a valid payload gets sent for the first time and it goes through
        test_round.process_payload(first_payload)

        # a duplicate (valid) payload will not go through
        with pytest.raises(
            TransactionNotValidError,
            match=f"sender {first_payload.sender} has already sent value for round",
        ):
            test_round.check_payload(first_payload)

        with pytest.raises(
            ABCIAppInternalError,
            match=f"sender {first_payload.sender} has already sent value for round",
        ):
            test_round.process_payload(first_payload)


class TestTransactionPreparationRound(BaseRoundTestClass):
    """Tests for TransactionPreparationRound."""

    _synchronized_data_class = SynchronizedData
    _event_class = Event
    _payload_data = "0xpayloadhash"

    def test_run_done(
        self,
    ) -> None:
        """Run tests."""

        test_round = TransactionPreparationRound(
            synchronized_data=deepcopy(self.synchronized_data),
            context=MagicMock(
                params=MagicMock(
                    default_chain_id=1,
                )
            ),
        )

        first_payload, *payloads = [
            TransactionPayload(sender=participant, content=self._payload_data)
            for participant in self.participants
        ]

        test_round.process_payload(first_payload)
        assert test_round.collection == {first_payload.sender: first_payload}
        assert test_round.end_block() is None

        for payload in payloads:
            test_round.process_payload(payload)

        expected_state = self.synchronized_data.update(
            most_voted_tx_hash=self._payload_data
        )

        res = test_round.end_block()
        assert res is not None
        actual_state, event = res

        assert (
            cast(SynchronizedData, actual_state).most_voted_tx_hash
            == cast(  # pylint: disable=no-member
                SynchronizedData, expected_state
            ).most_voted_tx_hash
        )

        assert event == Event.DONE

    def test_run_error(
        self,
    ) -> None:
        """Run tests."""

        test_round = TransactionPreparationRound(
            synchronized_data=deepcopy(self.synchronized_data),
            context=MagicMock(
                params=MagicMock(
                    default_chain_id=1,
                )
            ),
        )
        payload_data = TransactionPreparationRound.ERROR_PAYLOAD
        first_payload, *payloads = [
            TransactionPayload(sender=participant, content=payload_data)
            for participant in self.participants
        ]

        test_round.process_payload(first_payload)
        assert test_round.collection == {first_payload.sender: first_payload}
        assert test_round.end_block() is None

        for payload in payloads:
            test_round.process_payload(payload)

        res = test_round.end_block()
        assert res is not None
        actual_state, event = res

        with pytest.raises(ValueError):
            actual_state.most_voted_tx_hash  # pylint: disable=pointless-statement

        assert event == Event.ERROR

    def test_run_no_majority(
        self,
    ) -> None:
        """Run tests."""

        test_round = TransactionPreparationRound(
            synchronized_data=deepcopy(self.synchronized_data),
            context=MagicMock(
                params=MagicMock(
                    default_chain_id=1,
                )
            ),
        )
        first_payload, *payloads = [
            TransactionPayload(sender=participant, content=self._payload_data)
            for participant in self.participants
        ]

        test_round.process_payload(first_payload)
        assert test_round.collection == {first_payload.sender: first_payload}
        assert test_round.end_block() is None

        self._test_no_majority_event(test_round)

    def test_bad_payload(
        self,
    ) -> None:
        """Run tests."""

        test_round = TransactionPreparationRound(
            synchronized_data=deepcopy(self.synchronized_data),
            context=MagicMock(),
        )

        bad_participant = "non_existent"
        bad_participant_payload = TransactionPayload(
            sender=bad_participant, content=self._payload_data
        )
        first_payload, *_ = [
            TransactionPayload(sender=participant, content=self._payload_data)
            for participant in self.participants
        ]

        with pytest.raises(
            TransactionNotValidError,
            match=f"{bad_participant} not in list of participants",
        ):
            test_round.check_payload(bad_participant_payload)

        with pytest.raises(
            ABCIAppInternalError,
            match=f"{bad_participant} not in list of participants",
        ):
            test_round.process_payload(bad_participant_payload)

        # a valid payload gets sent for the first time and it goes through
        test_round.process_payload(first_payload)

        # a duplicate (valid) payload will not go through
        with pytest.raises(
            TransactionNotValidError,
            match=f"sender {first_payload.sender} has already sent value for round",
        ):
            test_round.check_payload(first_payload)

        with pytest.raises(
            ABCIAppInternalError,
            match=f"sender {first_payload.sender} has already sent value for round",
        ):
            test_round.process_payload(first_payload)
