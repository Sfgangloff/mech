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
from packages.valory.skills.subscription_abci.payloads import UpdateSubscriptionPayload
from packages.valory.skills.subscription_abci.rounds import (
    Event,
    SynchronizedData,
    UpdateSubscriptionRound,
)


class TestUpdateSubscriptionRound(BaseRoundTestClass):
    """Tests for UpdateSubscriptionRound."""

    _synchronized_data_class = SynchronizedData
    _event_class = Event

    def test_run_done(
        self,
    ) -> None:
        """Run tests."""

        test_round = UpdateSubscriptionRound(
            synchronized_data=deepcopy(self.synchronized_data),
            context=MagicMock(
                params=MagicMock(
                    default_chain_id=1,
                )
            ),
        )
        payload_data = "0xpayloadhash"
        first_payload, *payloads = [
            UpdateSubscriptionPayload(sender=participant, content=payload_data)
            for participant in self.participants
        ]

        test_round.process_payload(first_payload)
        assert test_round.collection == {first_payload.sender: first_payload}
        assert test_round.end_block() is None

        for payload in payloads:
            test_round.process_payload(payload)

        expected_state = self.synchronized_data.update(
            most_voted_tx_hash=payload_data,
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

        test_round = UpdateSubscriptionRound(
            synchronized_data=deepcopy(self.synchronized_data),
            context=MagicMock(
                params=MagicMock(
                    default_chain_id=1,
                )
            ),
        )
        payload_data = UpdateSubscriptionRound.ERROR_PAYLOAD
        first_payload, *payloads = [
            UpdateSubscriptionPayload(sender=participant, content=payload_data)
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

    def test_run_no_tx(
        self,
    ) -> None:
        """Run tests."""

        test_round = UpdateSubscriptionRound(
            synchronized_data=deepcopy(self.synchronized_data),
            context=MagicMock(
                params=MagicMock(
                    default_chain_id=1,
                )
            ),
        )
        payload_data = UpdateSubscriptionRound.NO_TX_PAYLOAD
        first_payload, *payloads = [
            UpdateSubscriptionPayload(sender=participant, content=payload_data)
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

        assert event == Event.NO_TX

    def test_run_no_majority(
        self,
    ) -> None:
        """Run tests."""

        test_round = UpdateSubscriptionRound(
            synchronized_data=deepcopy(self.synchronized_data),
            context=MagicMock(
                params=MagicMock(
                    default_chain_id=1,
                )
            ),
        )
        payload_data = "0xpayloadhash"
        first_payload, *payloads = [
            UpdateSubscriptionPayload(sender=participant, content=payload_data)
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

        test_round = UpdateSubscriptionRound(
            synchronized_data=deepcopy(self.synchronized_data),
            context=MagicMock(),
        )

        payload_data = "0xpayloadhash"
        bad_participant = "non_existent"
        bad_participant_payload = UpdateSubscriptionPayload(
            sender=bad_participant, content=payload_data
        )
        first_payload, *_ = [
            UpdateSubscriptionPayload(sender=participant, content=payload_data)
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
