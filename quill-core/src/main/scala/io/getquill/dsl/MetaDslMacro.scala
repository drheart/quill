package io.getquill.dsl

import io.getquill.util.Messages._
import scala.reflect.macros.whitebox.{ Context => MacroContext }
import io.getquill.util.OptionalTypecheck

class MetaDslMacro(val c: MacroContext) {
  import c.universe._

  def materializeQueryMeta[T](implicit t: WeakTypeTag[T]): Tree = {
    val value = this.value("Decoder", t.tpe)
    q"""
      new ${c.prefix}.QueryMeta[$t] {
        override val expand = ${expandQuery[T](flatten(value))}
        override val extract = ${extract[T](value)}
      }
    """
  }

  def materializeUpdateMeta[T](implicit t: WeakTypeTag[T]): Tree =
    q"""
      new ${c.prefix}.UpdateMeta[$t] {
        override val expand = ${expandAction[T]("update")}
      }
    """

  def materializeInsertMeta[T](implicit t: WeakTypeTag[T]): Tree =
    q"""
      new ${c.prefix}.InsertMeta[$t] {
        override val expand = ${expandAction[T]("insert")}
      }
    """

  private def expandQuery[T](scalars: List[Scalar])(implicit t: WeakTypeTag[T]) = {
    val elements = scalars.map(_.nest(q"x"))
    q"${c.prefix}.quote((q: ${c.prefix}.Query[$t]) => q.map(x => io.getquill.dsl.UnlimitedTuple(..$elements)))"
  }

  private def extract[T](value: Value)(implicit t: WeakTypeTag[T]): Tree = {
    var index = -1
    def expand(value: Value): Tree =
      value match {
        case Scalar(path, decoder) =>
          index += 1
          q"$decoder($index, row)"
        case Nested(tpe, params) =>
          q"new $tpe(...${params.map(_.map(expand))})"
      }
    q"(row: ${c.prefix}.ResultRow) => ${expand(value)}"
  }

  private def expandAction[T](method: String)(implicit t: WeakTypeTag[T]): Tree = {
    val assignments =
      flatten(value("Encoder", t.tpe)).map { scalar =>
        q"(v: $t) => ${scalar.nest(q"v")} -> ${scalar.nest(q"value")}"
      }
    q"${c.prefix}.quote((q: ${c.prefix}.EntityQuery[$t], value: $t) => q.${TermName(method)}(..$assignments))"
  }

  private def flatten(value: Value): List[Scalar] =
    value match {
      case Nested(tpe, params) => params.flatten.map(flatten).flatten
      case s: Scalar           => List(s)
    }

  sealed trait Value
  case class Nested(tpe: Type, params: List[List[Value]]) extends Value
  case class Scalar(path: List[TermName], decoder: Tree) extends Value {
    def nest(tree: Tree) =
      path.foldLeft(tree) {
        case (tree, term) => q"$tree.$term"
      }
  }

  private def value(encoding: String, tpe: Type): Value = {
    def apply(tpe: Type, path: List[TermName], nested: Boolean): Value = {
      OptionalTypecheck(c)(q"implicitly[${c.prefix}.${TypeName(encoding)}[$tpe]]") match {
        case Some(encoding) => Scalar(path, encoding)
        case None =>
          tpe.baseType(c.symbolOf[MetaDsl#Embedded]) match {
            case NoType if (nested) =>
              c.fail(
                s"Can't expand nested value '$tpe', please make it an `Embedded` " +
                  s"case class or provide an implicit $encoding for it."
              )
            case _ =>
              caseClassConstructor(tpe) match {
                case None =>
                  c.fail(s"Found the embedded '$tpe', but it is not a case class")
                case Some(constructor) =>
                  c.warn(tpe.toString())
                  val params =
                    constructor.paramLists.map {
                      _.map { param =>
                        apply(
                          param.typeSignature.asSeenFrom(tpe, tpe.typeSymbol),
                          path :+ param.name.toTermName,
                          nested = nested || !isTuple(tpe)
                        )
                      }
                    }
                  Nested(tpe, params)
              }
          }
      }
    }
    apply(tpe, path = Nil, nested = false)
  }

  private def isTuple(tpe: Type) =
    tpe.typeSymbol.name.toString.startsWith("Tuple")

  private def caseClassConstructor(t: Type) =
    t.members.collect {
      case m: MethodSymbol if (m.isPrimaryConstructor) => m
    }.headOption
}