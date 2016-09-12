using System;
using System.Data;
using System.Data.Common;
using System.Linq;
using System.Reflection;
using System.Text.RegularExpressions;

namespace Poncho
{
    public sealed class DataSourceInformation
    {
        #region Properties

        private static readonly Type _Type = typeof(DataSourceInformation);
        private static readonly Type _IdentifierCaseType = Enum.GetUnderlyingType(typeof(IdentifierCase));
        private static readonly Type _GroupByBehaviorType = Enum.GetUnderlyingType(typeof(GroupByBehavior));
        private static readonly Type _SupportedJoinOperatorsType = Enum.GetUnderlyingType(typeof(SupportedJoinOperators));
        private static readonly BindingFlags _flags = BindingFlags.IgnoreCase | BindingFlags.NonPublic | BindingFlags.Instance;

        private readonly string _compositeIdentifierSeparatorPattern = string.Empty;
        private readonly string _dataSourceProductName = string.Empty;
        private readonly string _dataSourceProductVersion = string.Empty;
        private readonly string _dataSourceProductVersionNormalized = string.Empty;
        private readonly GroupByBehavior _groupByBehavior;
        private readonly string _identifierPattern = string.Empty;
        private readonly IdentifierCase _identifierCase;
        private readonly bool _orderByColumnsInSelect = false;
        private readonly string _parameterMarkerFormat = string.Empty;
        private readonly string _parameterMarkerPattern = string.Empty;
        private readonly Int32 _parameterNameMaxLength = 0;
        private readonly string _parameterNamePattern = string.Empty;
        private readonly string _quotedIdentifierPattern = string.Empty;
        private readonly Regex _quotedIdentifierCase;
        private readonly string _statementSeparatorPattern = string.Empty;
        private readonly Regex _stringLiteralPattern;
        private readonly SupportedJoinOperators _supportedJoinOperators;
        private Regex _parameterNamePatternRegex;
        private string _parameterPrefix;
        private string _namedParameterMarker;

        public string CompositeIdentifierSeparatorPattern
        {
            get { return _compositeIdentifierSeparatorPattern; }
        }
        public string DataSourceProductName
        {
            get { return _dataSourceProductName; }
        }
        public string DataSourceProductVersion
        {
            get { return _dataSourceProductVersion; }
        }
        public string DataSourceProductVersionNormalized
        {
            get { return _dataSourceProductVersionNormalized; }
        }
        public GroupByBehavior GroupByBehavior
        {
            get { return _groupByBehavior; }
        }
        public string IdentifierPattern
        {
            get { return _identifierPattern; }
        }
        public IdentifierCase IdentifierCase
        {
            get { return _identifierCase; }
        }
        public bool OrderByColumnsInSelect
        {
            get { return _orderByColumnsInSelect; }
        }
        public string ParameterMarkerFormat
        {
            get { return _parameterMarkerFormat; }
        }
        public string ParameterMarkerPattern
        {
            get { return _parameterMarkerPattern; }
        }
        public int ParameterNameMaxLength
        {
            get { return _parameterNameMaxLength; }
        }
        public string ParameterNamePattern
        {
            get { return _parameterNamePattern; }
        }
        public string QuotedIdentifierPattern
        {
            get { return _quotedIdentifierPattern; }
        }
        public Regex QuotedIdentifierCase
        {
            get { return _quotedIdentifierCase; }
        }
        public string StatementSeparatorPattern
        {
            get { return _statementSeparatorPattern; }
        }
        public Regex StringLiteralPattern
        {
            get { return _stringLiteralPattern; }
        }
        public SupportedJoinOperators SupportedJoinOperators
        {
            get { return _supportedJoinOperators; }
        }
        public Regex ParameterNamePatternRegex
        {
            get
            {
                return _parameterNamePatternRegex ??
                    (_parameterNamePatternRegex = new Regex(ParameterNamePattern));
            }
        }
        public string ParameterMarker
        {
            get
            {
                if (string.IsNullOrEmpty(_parameterPrefix))
                {
                    _parameterPrefix = _parameterNameMaxLength != 0
                                        ? ParameterMarkerPattern.Substring(0, 1)
                                        : ParameterMarkerFormat;
                }
                return _parameterPrefix;
            }
        }
        public string NamedParameterMarker
        {
            get
            {
                if (string.IsNullOrEmpty(_namedParameterMarker))
                {
                    _namedParameterMarker = string.Empty;
                }
                return _namedParameterMarker;
            }
            set
            {
                _namedParameterMarker = (value ?? string.Empty).Trim();
            }
        }

        #endregion

        #region Constructors

        internal DataSourceInformation(DataTable infoTable)
        {
            if (infoTable == null)
                throw new ArgumentNullException("infoTable");

            DataRow infoRow = infoTable.AsEnumerable().FirstOrDefault(); // Schema should only be one row
            if (infoRow == null)
                throw new InvalidOperationException("InfoTable does not contain valid Data Source Information.");

            foreach (DataColumn col in infoTable.Columns)
            {
                string colName = col.ColumnName;
                object val = infoRow[col.ColumnName];

                if (val == DBNull.Value || val == null || string.IsNullOrEmpty(colName))
                    continue;

                switch (colName)
                {
                    case "QuotedIdentifierCase":
                        _quotedIdentifierCase = new Regex(val.ToString());
                        break;
                    case "StringLiteralPattern":
                        _stringLiteralPattern = new Regex(val.ToString());
                        break;
                    case "GroupByBehavior":
                        val = Convert.ChangeType(val, _GroupByBehaviorType);
                        _groupByBehavior = (GroupByBehavior)val;
                        break;
                    case "IdentifierCase":
                        val = Convert.ChangeType(val, _IdentifierCaseType);
                        _identifierCase = (IdentifierCase)val;
                        break;
                    case "SupportedJoinOperators":
                        val = Convert.ChangeType(val, _SupportedJoinOperatorsType);
                        _supportedJoinOperators = (SupportedJoinOperators)val;
                        break;
                    default:
                        FieldInfo fi = _Type.GetField("_" + colName, _flags);
                        if (fi != null)
                        {
                            fi.SetValue(this, val);
                        }
                        break;
                }

            }
        }

        #endregion
    }
}

