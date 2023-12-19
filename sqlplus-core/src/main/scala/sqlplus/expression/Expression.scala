package sqlplus.expression

import sqlplus.types.{DataType, DateDataType, DoubleDataType, IntDataType, IntervalDataType, LongDataType, StringDataType, TimestampDataType}

sealed trait Expression {
    def getType(): DataType

    def getVariables(): Set[Variable]

    def format(): String

    override def toString: String = format()
}

sealed trait ComputeExpression extends Expression {
    def getComputeFunction(variables: List[Variable], cast: Boolean = false): String => String
}

sealed trait LiteralExpression extends Expression {
    override def getVariables(): Set[Variable] = Set()

    def getLiteral(): String

    override def format(): String = getLiteral()
}

case class SingleVariableExpression(variable: Variable) extends ComputeExpression {
    override def getType(): DataType = variable.dataType

    override def getVariables(): Set[Variable] = Set(variable)

    override def getComputeFunction(variables: List[Variable], cast: Boolean = false): String => String = x => {
        val raw = x + "(" + variables.indexOf(variable) + ")"
        if (cast) getType().castFromAny(raw) else raw
    }

    override def format(): String = variable.name
}

abstract class BinaryExpression(left: Expression, right: Expression, operator: String, returnType: DataType) extends ComputeExpression{
    override def getType(): DataType = returnType

    override def getVariables(): Set[Variable] = left.getVariables() ++ right.getVariables()

    override def getComputeFunction(variables: List[Variable], cast: Boolean = false): String => String = x => {
        val leftOperand = left match {
            case v: ComputeExpression => v.getComputeFunction(variables, true)(x)
            case l: LiteralExpression => l.getLiteral()
        }
        val rightOperand = right match {
            case v: ComputeExpression => v.getComputeFunction(variables, true)(x)
            case l: LiteralExpression => l.getLiteral()
        }
        val raw = s"($leftOperand $operator $rightOperand)"
        if (cast) getType().castFromAny(raw) else raw
    }

    override def format(): String = s"(${left.format()} ${operator} ${right.format()})"
}

case class IntPlusIntExpression(left: Expression, right: Expression) extends BinaryExpression(left, right, "+", IntDataType)
case class LongPlusLongExpression(left: Expression, right: Expression) extends BinaryExpression(left, right, "+", LongDataType)
case class TimestampPlusIntervalExpression(left: Expression, right: Expression) extends BinaryExpression(left, right, "+", TimestampDataType)
case class DatePlusIntervalExpression(left: Expression, right: Expression) extends BinaryExpression(left, right, "+", DateDataType)
case class DoublePlusDoubleExpression(left: Expression, right: Expression) extends BinaryExpression(left, right, "+", DoubleDataType)

case class IntMinusIntExpression(left: Expression, right: Expression) extends BinaryExpression(left, right, "-", IntDataType)
case class LongMinusLongExpression(left: Expression, right: Expression) extends BinaryExpression(left, right, "-", LongDataType)
case class TimestampMinusIntervalExpression(left: Expression, right: Expression) extends BinaryExpression(left, right, "-", TimestampDataType)
case class DateMinusIntervalExpression(left: Expression, right: Expression) extends BinaryExpression(left, right, "-", DateDataType)
case class DoubleMinusDoubleExpression(left: Expression, right: Expression) extends BinaryExpression(left, right, "-", DoubleDataType)

case class IntTimesIntExpression(left: Expression, right: Expression) extends BinaryExpression(left, right, "*", IntDataType)
case class LongTimesLongExpression(left: Expression, right: Expression) extends BinaryExpression(left, right, "*", LongDataType)
case class DoubleTimesDoubleExpression(left: Expression, right: Expression) extends BinaryExpression(left, right, "*", DoubleDataType)

case class IntDivideByIntExpression(left: Expression, right: Expression) extends BinaryExpression(left, right, "/", IntDataType)
case class LongDivideByLongExpression(left: Expression, right: Expression) extends BinaryExpression(left, right, "/", LongDataType)
case class DoubleDivideByDoubleExpression(left: Expression, right: Expression) extends BinaryExpression(left, right, "/", DoubleDataType)

case class StringLiteralExpression(lit: String) extends LiteralExpression {
    override def getLiteral(): String = "\"" + lit + "\""

    override def getType(): DataType = StringDataType
}

case class IntLiteralExpression(lit: Int) extends LiteralExpression {
    override def getLiteral(): String = s"$lit"

    override def getType(): DataType = IntDataType
}

case class LongLiteralExpression(lit: Long) extends LiteralExpression {
    override def getLiteral(): String = s"$lit"

    override def getType(): DataType = IntDataType
}

case class DoubleLiteralExpression(lit: Double) extends LiteralExpression {
    override def getLiteral(): String = s"${lit}d"

    override def getType(): DataType = DoubleDataType
}

case class IntervalLiteralExpression(lit: Long) extends LiteralExpression {
    val ms = s"${lit}L"
    override def getLiteral(): String = ms

    override def getType(): DataType = IntervalDataType
}

case class DateLiteralExpression(lit: Long) extends LiteralExpression {
    val ms = s"${lit}L"
    override def getLiteral(): String = ms

    override def getType(): DataType = DateDataType
}

case class CaseWhenExpression(branches: List[(Operator, List[Expression], Expression)], default: Expression) extends Expression {
    assert(branches.forall(t => t._3.getType() == default.getType()))

    override def getType(): DataType = default.getType()

    override def getVariables(): Set[Variable] = branches.flatMap(t => t._3.getVariables()).toSet ++ default.getVariables()

    override def format(): String = {
        val lines = branches.map(t => s"WHEN ${t._1.format(t._2)} THEN ${t._3.format()}").mkString(" ")
        s"CASE ${lines} ELSE ${default.format()} END"
    }
}
