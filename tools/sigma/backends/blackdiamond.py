
import re
import sigma
from .base import SingleTextQueryBackend
from .mixins import MultiRuleOutputMixin

class BlackDiamondBackend(SingleTextQueryBackend):
    """Converts Sigma rule into Splunk Search Processing Language (SPL)."""
    identifier = "bdiamond"
    active = True