# -*- coding: utf-8 -*-
# ------------------------------------------------------------------------------
#
#   Copyright 2023 valory
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

"""
This module contains the classes required for mech_acn dialogue management.

- MechAcnDialogue: The dialogue class maintains state of a dialogue and manages it.
- MechAcnDialogues: The dialogues class keeps track of all dialogues.
"""

from abc import ABC
from typing import Callable, Dict, FrozenSet, Type, cast

from aea.common import Address
from aea.protocols.base import Message
from aea.protocols.dialogue.base import Dialogue, DialogueLabel, Dialogues

from packages.valory.protocols.mech_acn.message import MechAcnMessage


class MechAcnDialogue(Dialogue):
    """The mech_acn dialogue class maintains state of a dialogue and manages it."""

    INITIAL_PERFORMATIVES: FrozenSet[Message.Performative] = frozenset(
        {MechAcnMessage.Performative.REQUEST}
    )
    TERMINAL_PERFORMATIVES: FrozenSet[Message.Performative] = frozenset(
        {MechAcnMessage.Performative.RESPONSE}
    )
    VALID_REPLIES: Dict[Message.Performative, FrozenSet[Message.Performative]] = {
        MechAcnMessage.Performative.REQUEST: frozenset(
            {MechAcnMessage.Performative.RESPONSE}
        ),
        MechAcnMessage.Performative.RESPONSE: frozenset(),
    }

    class Role(Dialogue.Role):
        """This class defines the agent's role in a mech_acn dialogue."""

        AGENT = "agent"
        SKILL = "skill"

    class EndState(Dialogue.EndState):
        """This class defines the end states of a mech_acn dialogue."""

        SUCCESSFUL = 0
        FAILED = 1

    def __init__(
        self,
        dialogue_label: DialogueLabel,
        self_address: Address,
        role: Dialogue.Role,
        message_class: Type[MechAcnMessage] = MechAcnMessage,
    ) -> None:
        """
        Initialize a dialogue.

        :param dialogue_label: the identifier of the dialogue
        :param self_address: the address of the entity for whom this dialogue is maintained
        :param role: the role of the agent this dialogue is maintained for
        :param message_class: the message class used
        """
        Dialogue.__init__(
            self,
            dialogue_label=dialogue_label,
            message_class=message_class,
            self_address=self_address,
            role=role,
        )


class MechAcnDialogues(Dialogues, ABC):
    """This class keeps track of all mech_acn dialogues."""

    END_STATES = frozenset(
        {MechAcnDialogue.EndState.SUCCESSFUL, MechAcnDialogue.EndState.FAILED}
    )

    _keep_terminal_state_dialogues = False

    def __init__(
        self,
        self_address: Address,
        role_from_first_message: Callable[[Message, Address], Dialogue.Role],
        dialogue_class: Type[MechAcnDialogue] = MechAcnDialogue,
    ) -> None:
        """
        Initialize dialogues.

        :param self_address: the address of the entity for whom dialogues are maintained
        :param dialogue_class: the dialogue class used
        :param role_from_first_message: the callable determining role from first message
        """
        Dialogues.__init__(
            self,
            self_address=self_address,
            end_states=cast(FrozenSet[Dialogue.EndState], self.END_STATES),
            message_class=MechAcnMessage,
            dialogue_class=dialogue_class,
            role_from_first_message=role_from_first_message,
        )
