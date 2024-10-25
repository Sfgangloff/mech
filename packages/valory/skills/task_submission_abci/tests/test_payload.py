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

"""Test the payloads.py module of the skill."""


from packages.valory.skills.task_submission_abci.payloads import (
    TaskPoolingPayload,
    TransactionPayload,
)


def test_task_pooling_payload() -> None:
    """Test `TaskPoolingPayload`."""
    content = "donetaskjsonstr"
    payload = TaskPoolingPayload(sender="sender", content=content)

    assert payload.content == content
    assert payload.data == {"content": content}


def test_transaction_payload() -> None:
    """Test `TransactionPayload`."""
    content = "0xpayloadhash"
    payload = TransactionPayload(sender="sender", content=content)

    assert payload.content == content
    assert payload.data == {"content": content}
