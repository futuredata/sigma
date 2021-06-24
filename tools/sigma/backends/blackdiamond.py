import re
from typing import ItemsView, Pattern
import sigma
from fnmatch import fnmatch
from .base import SingleTextQueryBackend
from sigma.parser.exceptions import SigmaParseError
from sigma.parser.condition import SigmaAggregationParser, NodeSubexpression, ConditionAND, ConditionOR, ConditionNOT
from sigma.parser.modifiers.type import SigmaRegularExpressionModifier, SigmaTypeModifier

class BlackDiamondBackend(SingleTextQueryBackend):
    """Converts Sigma rule into Black Diamond Correlation Rule"""
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
    fulltextSearchField = "einfo"
    whenClause = '1 event'
    havingClauseFields = ['tenantname', 'obsname', 'obsip']
    sevMapping = {'informational': '24h', 'low': '18h', 'medium': '12h', 'high': '6h', 'critical': '3h'}
    additionalWhereClause = 'tenantname NOT IN "tenantnameProvisioning"'

    def __init__(self, sigmaconfig, options):
        super().__init__(sigmaconfig)

    def generateANDNode(self, node):
        generated = []
        for val in node:
            if(type(val)==str):
                val=(self.fulltextSearchField, '*' + val + "*")
            generated.append(self.generateNode(val))
        #generated = [ self.generateNode(val=('keyword', val) if type(val)=="str" else val) for val in node ]
        filtered = [ g for g in generated if g is not None ]
        print("visit ANDnode")
        if filtered:
            return self.andToken.join(filtered)
        else:
            return None

    def generateORNode(self, node):
        generated = []
        for val in node:
            if(type(val)==str):
                val=(self.fulltextSearchField, '*' + val + "*")
            generated.append(self.generateNode(val))
        #generated = [ self.generateNode(val=('keyword', val) if type(val)=="str" else val) for val in node ]
        filtered = [ g for g in generated if g is not None ]
        print("visit ORnode")
        if filtered:
            return self.orToken.join(filtered)
        else:
            return None

    def generateNOTNode(self, node):
        generated = self.generateNode(node.item)
        if generated is not None:
            pattern = r"\(\(([A-Za-z-_]+)\s((?:LIKE\s(?:\'\S%?.*%?\S\'))|(?:IN\s\((?:.+(?:,)?){1,}\))|(?:MATCH\sREGEX\(\"(?:.*)\"\))|(?:IS NULL)|(?:\=\s(\'(?:\S+)\')))\)\)"
            patternAString = r"\(\((\'[^\']+\'|\"[^\"]+\")\)\)"
            if(re.search(pattern , generated)):
                generated = re.sub(pattern, r"(\1 \2)", generated)
            elif(re.search(patternAString, generated)):
                generated = re.sub(patternAString, r"(\1)", generated)
            return self.formatQuery(self.notToken + generated)
        else:
            return None

    def generateSubexpressionNode(self, node):
        return super().generateSubexpressionNode(node)

    def generateListNode(self, node):
        print("visit ListNode")
        if not set([type(value) for value in node]).issubset({str, int}):
            raise TypeError("List values must be strings or numbers")
        return self.listExpression % (self.listSeparator.join([self.generateNode(value) for value in node]))

    def makeCaseInSensitiveValue(self, value):
        """
        Returns dictionary of if should be a regex (`is_regex`) and if regex the query value ('value')
        Converts the query(value) into a case insensitive regular expression (regex). ie: 'http' would get converted to '[hH][tT][pP][pP]'
        Adds the beginning and ending '/' to make regex query if still determined that it should be a regex
        """
        if value and not value == 'null' and not re.match(r'^/.*/$', value) and (re.search('[a-zA-Z]', value) and not re.match(self.uuid_regex, value)):  # re.search for alpha is fastest:
            # Escape additional values that are treated as specific "operators" within Elastic. (ie: @, ?, &, <, >, and ~)
            # reference: https://www.elastic.co/guide/en/elasticsearch/reference/current/regexp-syntax.html#regexp-optional-operators
            value = re.sub(r"(((?<!\\)(\\\\)+)|(?<!\\))([@?&~<>])", "\g<1>\\\\\g<4>", value)
            # Validate regex
            try:
                re.compile(value)
                return {'is_regex': True, 'value': value}
            # Regex failed
            except re.error:
                raise TypeError( "Regular expression validation error for: '%s')" %str(value) )
        else:
            return { 'is_regex': False, 'value': value }

    def generateTypedValueNode(self, node):
        try:
            return self.typedValueExpression % (str(node))
        except KeyError:
            raise NotImplementedError("Type modifier '{}' is not supported by backend".format(node.identifier))

    def generateMapItemNode(self, node):
        print("visit MapItemNode")
        fieldname, value = node
        transformed_fieldname = self.fieldNameMapping(fieldname, value)

        has_wildcard = False
        if value is not None:
            has_wildcard = re.search(r"((\\(\*|\?))|\*|\?|%)", self.generateNode(value))

        if isinstance(value, SigmaRegularExpressionModifier):
            return self.mapSource % (transformed_fieldname, self.generateNode(value))
        elif type(value) == list:
            return self.generateMapItemListNode(transformed_fieldname, value)
        elif self.mapListsSpecialHandling == False and type(value) in (str, int, list) or self.mapListsSpecialHandling == True and type(value) in (str, int):
            if has_wildcard:
                return self.mapSource % (transformed_fieldname , (self.mapWildcard % self.generateNode(value)))
            else:
                return self.equalSource % (transformed_fieldname, self.generateNode(value))
        elif "sourcetype" in transformed_fieldname:
            return self.equalSource % (transformed_fieldname, self.generateNode(value))
        elif has_wildcard:
            return self.mapWildcard % self.generateNode(value)
        elif value is None:
            return self.nullExpression % transformed_fieldname
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
    
        if isinstance(value, SigmaRegularExpressionModifier):
            self.matchKeyword = True
        else:
            self.matchKeyword = False

        self.keyword_field = ''

        if self.matchKeyword:
            return '%s%s'%(fieldname, '')
        else:
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
        val = re.sub(r"(?<!\\)(\\\\)*(?!\\)(?<!\*)\*", r"\1%", val)
        return val

    def generate(self, sigmaparser):
        """Method is called for each sigma rule and receives the parsed rule (SigmaParser)"""
        for parsed in sigmaparser.condparsed:
            query = self.generateQuery(parsed, sigmaparser)
            before = self.generateBefore(parsed)
            after = self.generateAfter(parsed)

            result = ""
            if before is not None:
                result = before
            if query is not None:
                result += query
            if after is not None:
                result += after

            return result

    def generateQuery(self, parsed, sigmaparser):
        result = self.addToEndOfQuery(self.formatQuery(self.generateNode(parsed.parsedSearch)))
        try:
            timeframe = sigmaparser.parsedyaml['detection']['timeframe']
        except:
            timeframe = None
        try:
            sev = sigmaparser.parsedyaml['level']
        except:
            sev = None
        #Handle aggregation
        when, whe, having = self.generateAggregation(parsed.parsedAgg, result, self.havingClauseFields)
        ruleParsed = "WHEN {} WHERE {}"
        if(timeframe != None) :
            ruleParsed += (" WITHIN {}".format(timeframe))
        ruleParsed += " HAVING SAME {} "
        if(sev != None):
            ruleParsed += ("SUPPRESS {}".format(self.sevMapping[sev]))
        return ruleParsed.format(when, whe, ','.join(having))

    def generateAggregation(self, agg, where_clausel, having):
        if not agg:
            return self.whenClause, where_clausel, having

        if  (agg.aggfunc == SigmaAggregationParser.AGGFUNC_COUNT):

            if agg.groupfield:
                having.append(agg.groupfield)
            if (agg.cond_op == '>' or agg.cond_op == '>='):
                when = ""
                if(agg.cond_op == '>='):
                    when += (agg.condition + " event")
                else:
                    when += (str(int(agg.condition) + 1) + ' events')
                return when, where_clausel, having
        else:
            return self.whenClause, where_clausel, having
        raise NotImplementedError("{} aggregation not implemented in BD Backend".format(agg.aggfunc_notrans))


    def generateNode(self, node):
        #Save fields for adding them in query_key
        #if type(node) == sigma.parser.NodeSubexpression:
        #    for k,v in node.items.items:
        #        self.fields.append(k)
        return super().generateNode(node)

    def formatQuery(self, query):
        #Replace NOT key LIKE | NOT key IN | NOT key MATCH REGEX => key NOT LIKE|IN|MATCH REGEX
        query = re.sub(r"NOT\s(?:\()([A-Za-z-_]+)\s((?:LIKE\s(?:\'\S%?.*%?\S\'))|(?:IN\s\((?:.+(?:,)?){1,}\))|(?:MATCH\sREGEX\(\"(?:.*)\"\)))(?:\))", r"(\1 NOT \2)", query)
        #Replace NOT key = value => key != value
        query = re.sub(r"NOT\s(?:\()([A-Za-z-_]+)\s(?:\=\s(\'(?:\S+)\'))(?:\))", r"(\1 != \2)", query)
        #Replace NOT key IS NULL => key IS NOT NULL
        query = re.sub(r"NOT\s(?:\()([A-Za-z-_]+)\s(?:IS NULL)(?:\))", r"(\1 IS NOT NULL)", query)
        return query

    def addToEndOfQuery(self, query):
         #add tenant condition to the end of where clause
        query = re.sub(r"\)$", " AND " + self.additionalWhereClause + ")", query)
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