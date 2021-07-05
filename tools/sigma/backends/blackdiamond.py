import re
from sigma.tools import doIfDebug
from typing import ItemsView, Pattern
import sigma
from fnmatch import fnmatch
from .base import SingleTextQueryBackend
from sigma.config.mapping import ConditionalFieldMapping
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
    equalSource = "%s=%s"
    notEqualSource = "%s!=%s"
    typedValueExpression = "MATCH REGEX(\"%s\")"
    nullExpression = "%s IS NULL"
    notNullExpression = "%s IS NOT NULL"
    whenClause = '1 event'

    def __init__(self, sigmaconfig, options):
        super().__init__(sigmaconfig)
        if options['general']:
            self.fulltextSearchField = options['general']['fulltextSearchField']
            self.sevMapping = options['general']['sevMapping']
            self.havingClauseFields = options['general']['havingClauseFields']
            self.additionalWhereClause = options['general']['additionalWhereClause']
            self.sevMappingAsNum = options['general']['sevMappingAsNum']
            self.outputCSV = options['outputCSV']
        if options['others']:
            self.additionalWithCondition = options['others']

    def generateANDNode(self, node):
        generated = []
        for val in node:
            if(type(val)==str):
                val=(self.fulltextSearchField, '*' + val + "*")
            generated.append(self.generateNode(val))
        #generated = [ self.generateNode(val=('keyword', val) if type(val)=="str" else val) for val in node ]
        filtered = [ g for g in generated if g is not None ]

        doIfDebug(lambda : print("visit ANDnode"))

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
        doIfDebug(lambda : print("visit ANDnode"))
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
        doIfDebug(lambda: print("visit ListNode"))
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
        doIfDebug(lambda: print("visit MapItemNode") )
        fieldname, value = node
        transformed_fieldname = self.fieldNameMapping(fieldname, value)

        has_wildcard = False
        if value is not None:
            has_wildcard = re.search(r"((\\(\*|\?))|\*|\?|%)", self.generateNode(value))
            if(has_wildcard and type(value) == str):
                if(len(value)==1):
                    has_wildcard = None

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
    
        if isinstance(fieldname, str):
            get_config = self.sigmaconfig.fieldmappings.get(fieldname)
            if not get_config and '|' in fieldname:
                fieldname = fieldname.split('|', 1)[0]
                get_config = self.sigmaconfig.fieldmappings.get(fieldname)
            if isinstance(get_config, ConditionalFieldMapping):
                condition = self.sigmaconfig.fieldmappings.get(fieldname).conditions
                for key, item in self.logsource.items():
                    if condition.get(key) and condition.get(key, {}).get(item):
                        new_fieldname = condition.get(key, {}).get(item)
                        if any(new_fieldname):
                           return super().fieldNameMapping(new_fieldname[0], value)
        return super().fieldNameMapping(fieldname, value)

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
        if(len(val)>1 and re.search(r"(?<!\\)(\\\\)*(?!\\)(?<!\*)\*", val)):
            val = re.sub(r"(?<!\\)(\\\\)*(?!\\)(?<!\*)\*", r"\1%", val)
        return val

    def generate(self, sigmaparser):
        """Method is called for each sigma rule and receives the parsed rule (SigmaParser)"""
        if len(sigmaparser.condparsed) > 1:
            query = ""
            for idx, parsed in enumerate(sigmaparser.condparsed):
                if(idx != 0):
                    query += "\nUNION OR\n"
                query += self.generateQuery(parsed, sigmaparser)
        else:
            query = self.generateQuery(sigmaparser.condparsed[0], sigmaparser)
        before = self.generateBefore(sigmaparser)
        after = self.generateAfter(sigmaparser)

        result = []
        if(self.outputCSV):
            if before is not None:
                result.append(before)
            if query is not None:
                result.append("\"" + query + "\"")
            if after is not None:
                result.append(after)
        else:
            if query is not None:
                result.append(query)

        return ','.join(result)

    def generateQuery(self, parsed, sigmaparser):
        self.logsource = sigmaparser.parsedyaml['logsource']
        result = self.addToEndOfQuery(self.formatQuery(self.generateNode(parsed.parsedSearch)), sigmaparser.parsedyaml['logsource'])
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
        ruleParsed = "WHEN {}\n\tWHERE {}"
        if(timeframe != None) :
            ruleParsed += ("\n\tWITHIN {}".format(timeframe))
        ruleParsed += "\n\tHAVING SAME {} "
        if(sev != None):
            ruleParsed += ("\n\tSUPPRESS {}".format(self.sevMapping[sev]))
        if(self.outputCSV):
            return re.sub(r"\"", "\"\"", ruleParsed.format(when, whe, ','.join(having)))
        else:
            return ruleParsed.format(when, whe, ','.join(having))

    def formatStringInCSV(self, string):
        string = re.sub(r"\"", "\"\"", string)
        string = re.sub(r"\,", "\;", string)
        return string

    def generateBefore(self, sigmaparser):
        parseContent = []
        if('id' in sigmaparser.parsedyaml):
            parseContent.append("\"" + sigmaparser.parsedyaml['id'] + "\"") 
        parseContent.append("\"0\"")
        parseContent.append("\"\"")
        if('title' in sigmaparser.parsedyaml):
            parseContent.append("\"" + self.formatStringInCSV(sigmaparser.parsedyaml['title']) + "\"")
        if('description' in sigmaparser.parsedyaml):
            parseContent.append("\"" + self.formatStringInCSV(sigmaparser.parsedyaml['description']) + "\"")
        if('falsepositives' in sigmaparser.parsedyaml):
            parseContent.append("\"" + self.formatStringInCSV("\n".join(sigmaparser.parsedyaml['falsepositives'])) + "\"")
        parseContent.append("\"\"")
        parseContent.append("\"\"")
        if('level' in sigmaparser.parsedyaml):
            parseContent.append("\"" + str(self.sevMappingAsNum[sigmaparser.parsedyaml['level']]) + "\"")
        return ','.join(parseContent)

    def generateAfter(self, sigmaparser):
        parseContent = []
        if("status" in sigmaparser.parsedyaml):
            if(sigmaparser.parsedyaml['status'] == "experimental"):
                parseContent.append("\"true\"")
            else:
                parseContent.append("\"false\"")
        else: parseContent.append("\"false\"")
        parseContent.append("\"0\"")
        parseContent.append("\"0\"")
        parseContent.append("\"0\"")
        parseContent.append("\"0\"")
        parseContent.append("\"0\"")
        parseContent.append("\"0\"")
        parseContent.append("\"0\"")
        parseContent.append("\"0\"")
        parseContent.append("\"0\"")
        parseContent.append("\"0\"")
        parseContent.append("\"N\"")
        parseContent.append("\"-\"")
        parseContent.append("\"0\"")
        parseContent.append("\"0\"")
        parseContent.append("\"0\"")
        parseContent.append("\"N\"")
        parseContent.append("\"[]\"")
        return ','.join(parseContent)

    def generateAggregation(self, agg, where_clause, having):
        if not agg:
            return self.whenClause, where_clause, having

        if  (agg.aggfunc == SigmaAggregationParser.AGGFUNC_COUNT):

            if agg.groupfield:
                having.append(agg.groupfield)
            if (agg.cond_op == '>' or agg.cond_op == '>='):
                when = ""
                if(agg.cond_op == '>='):
                    if(int(agg.condition == 1)):
                        when += (agg.condition + " event")
                    else:
                        when += (agg.condition + " events")
                else:
                    when += (str(int(agg.condition) + 1) + ' events')
                return when, where_clause, having
            else: 
                return self.whenClause, where_clause, having
        else:
            return self.whenClause, where_clause, having
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
        query = re.sub(r"NOT\s(?:\()([A-Za-z-_]+)(?:\=(\'(?:\S+)\'))(?:\))", r"(\1 != \2)", query)
        #Replace NOT key IS NULL => key IS NOT NULL
        query = re.sub(r"NOT\s(?:\()([A-Za-z-_]+)\s(?:IS NULL)(?:\))", r"(\1 IS NOT NULL)", query)
        return query

    def addToEndOfQuery(self, query, logsource):
         #add tenant condition to the end of where clause
        product = service = ""
        if 'product' in logsource:
            productName = list(self.additionalWithCondition['product'].keys())
            if(logsource['product'] in productName):
                product = " AND " + self.additionalWithCondition['product'][logsource['product']]
        if 'service' in logsource:
            serviceName = list(self.additionalWithCondition['service'].keys())
            if(logsource['service'] in serviceName): 
                service = " AND " + self.additionalWithCondition['service'][logsource['service']]
        if(re.search(r"\)$", query)):
            query = re.sub(r"\)$", " AND " + self.additionalWhereClause + product + service + ")", query)
        else:
            query += (" AND " + self.additionalWhereClause + product + service)
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