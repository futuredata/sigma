import re
import sigma
from .base import SingleTextQueryBackend
from sigma.parser.condition import NodeSubexpression, ConditionAND, ConditionOR, ConditionNOT
from sigma.parser.modifiers.type import SigmaRegularExpressionModifier, SigmaTypeModifier

class BlackDiamondBackend(SingleTextQueryBackend):
    """Converts Sigma rule into Splunk Search Processing Language (SPL)."""
    identifier = "bdiamond"
    active = True

    andToken = " AND "
    orToken = " OR "
    notToken = "NOT "

    reEscape = re.compile("([\s+\\-=!(){}\\[\\]^\"~:/]|(?<!\\\\)\\\\(?![*?\\\\])|\\\\u|&&|\\|\\|)")
    subExpression = "(%s)"
    listExpression = "(%s)"
    listSeparator = ", "
    valueExpression = "\'%s\'"
    mapListValueExpression = "%s IN %s"
    mapWildcard = "LIKE %s"
    mapSource = "%s %s"
    equalSource = "%s = %s"
    notEqualSource = "%s != %s"
    typedValueExpression = "MATCH REGEX(\"%s\")"
    nullExpression = "%s IS NULL"
    notNullExpression = "%s IS NOT NULL"

    def __init__(self, sigmaconfig, options):
        super().__init__(sigmaconfig)

    def generateANDNode(self, node):
        generated = [ self.generateNode(val) for val in node ]
        filtered = [ g for g in generated if g is not None ]
        if filtered:
            return self.andToken.join(filtered)
        else:
            return None

    def generateORNode(self, node):
        generated = [ self.generateNode(val) for val in node ]
        filtered = [ g for g in generated if g is not None ]
        if filtered:
            return self.orToken.join(filtered)
        else:
            return None

    def generateNOTNode(self, node):
        generated = self.generateNode(node.item)
        if generated is not None:
            return self.notToken + generated
        else:
            return None

    def generateSubexpressionNode(self, node):
        generated = self.generateNode(node.items)
        if generated:
            return self.subExpression % generated
        else:
            return None

    def generateListNode(self, node):
        if not set([type(value) for value in node]).issubset({str, int}):
            raise TypeError("List values must be strings or numbers")
        return self.listExpression % (self.listSeparator.join([self.generateNode(value) for value in node]))

    def generateMapItemNode(self, node):
        fieldname, value = node
        transformed_fieldname = self.fieldNameMapping(fieldname, value)

        has_wildcard = re.search(r"((\\(\*|\?|\\))|\*|\?|_|%)", self.generateNode(value))

        if "," in self.generateNode(value) and not has_wildcard:
            return self.mapMulti % (transformed_fieldname, self.generateNode(value))
        elif type(value) == list:
            return self.generateMapItemListNode(transformed_fieldname, value)
        elif self.mapListsSpecialHandling == False and type(value) in (str, int, list) or self.mapListsSpecialHandling == True and type(value) in (str, int):
            if has_wildcard:
                return self.mapSource % (transformed_fieldname , (self.mapWildcard % self.generateNode(value)))
            else:
                return self.equalSource % (transformed_fieldname, self.generateNode(value))
        elif isinstance(value, SigmaRegularExpressionModifier):
            return self.mapSource % (transformed_fieldname, self.typedValueExpression)
        elif "sourcetype" in transformed_fieldname:
            return self.equalSource % (transformed_fieldname, self.generateNode(value))
        elif has_wildcard:
            return self.mapWildcard % self.generateNode(value)
        else:
            raise TypeError("Backend does not support map values of type " + str(type(value)))

    def generateMapItemListNode(self, key, value):
        return "(" + (" OR ".join([self.mapSource % (key, self.mapWildcard % self.generateValueNode(item)) for item in value])) + ")"

    def generateValueNode(self, node):
        return self.valueExpression % (self.cleanValue(str(node)))

    def generateNULLValueNode(self, node):
        return self.nullExpression % (node.item)

    def generateNotNULLValueNode(self, node):
        return self.notNullExpression % (node.item)

    def fieldNameMapping(self, fieldname, value):
        """
        Alter field names depending on the value(s). Backends may use this method to perform a final transformation of the field name
        in addition to the field mapping defined in the conversion configuration. The field name passed to this method was already
        transformed from the original name given in the Sigma rule.
        """
        return fieldname

    def cleanValue(self, val):
        if not isinstance(val, str):
            return str(val)

        #Single backlashes which are not in front of * or ? are doulbed
        val = re.sub(r"(?<!\\)\\(?!(\\|\*|\?))", r"\\\\", val)

        #Replace _ with \_ because _ is a sql wildcard
        val = re.sub(r'_', r'\_', val)

        #Replace % with \% because % is a sql wildcard
        val = re.sub(r'%', r'\%', val)

        #Replace * with %, if even number of backslashes (or zero) in front of *
        val = re.sub(r"(?<!\\)(\\\\)*(?!\\)\*", r"\1%", val)
        return val

    def generateQuery(self, parsed):
        if self._recursiveFtsSearch(parsed.parsedSearch):
            raise NotImplementedError("FullTextSearch not implemented for SQL Backend.")
        result = self.generateNode(parsed.parsedSearch)
        return self.formatQuery(result)

    def formatQuery(self, query):
        query = re.sub(r"NOT\s([A-Za-z-_]+)\s((?:LIKE\s(?:\'\S%?.+%?\S\'))|(?:IN\s\((?:.+(?:,)?){1,}\))|(?:MATCH\sREGEX\(\"(?:.+)\"\)))", r"\1 NOT \2", query)
        return query

    def _recursiveFtsSearch(self, subexpression):
        #True: found subexpression, where no fieldname is requested -> full text search
        #False: no subexpression found, where a full text search is needed

        def _evaluateCondition(condition):
            #Helper function to evaluate conditions
            if type(condition) not in  [ConditionAND, ConditionOR, ConditionNOT]:
                raise NotImplementedError("Error in recursive Search logic")

            results = []
            for elem in condition.items:
                if isinstance(elem, NodeSubexpression):
                    results.append(self._recursiveFtsSearch(elem))
                if isinstance(elem, ConditionNOT):
                    results.append(_evaluateCondition(elem))
                if isinstance(elem, tuple):
                    results.append(False)
                if type(elem) in (str, int, list):
                    return True
            return any(results)

        if type(subexpression) in [str, int, list]:
            return True
        elif type(subexpression) in [tuple]:
            return False

        if not isinstance(subexpression, NodeSubexpression):
            raise NotImplementedError("Error in recursive Search logic")

        if isinstance(subexpression.items, NodeSubexpression):
            return self._recursiveFtsSearch(subexpression.items)
        elif type(subexpression.items) in [ConditionAND, ConditionOR, ConditionNOT]:
            return _evaluateCondition(subexpression.items)