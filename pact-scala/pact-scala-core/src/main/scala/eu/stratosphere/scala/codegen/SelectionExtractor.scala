package eu.stratosphere.scala.codegen
import scala.reflect.macros.Context
import scala.Option.option2Iterable
import eu.stratosphere.scala.analysis.FieldSelector

trait SelectionExtractor[C <: Context] { this: MacroContextHolder[C] with UDTDescriptors[C] with UDTAnalyzer[C] with Loggers[C] with TreeGen[C] =>
  import c.universe._

  def getSelector[T: c.WeakTypeTag, R: c.WeakTypeTag](fun: c.Expr[T => R]): Expr[List[Int]] =
    (new SelectionExtractorInstance with Logger).extract(fun)

  class SelectionExtractorInstance { this: Logger =>
    def extract[T: c.WeakTypeTag, R: c.WeakTypeTag](fun: c.Expr[T => R]): Expr[List[Int]] = {
      val result = getSelector(fun.tree) match {
        case Left(errs) => Left(errs.toList)
        case Right(sels) => getUDTDescriptor(weakTypeOf[T]) match {
          case UnsupportedDescriptor(id, tpe, errs) => Left(errs.toList)
          case desc: UDTDescriptor => chkSelectors(desc, sels) match {
            case Nil => Right(desc, sels map { _.tail })
            case errs => Left(errs)
          }
        }
      }

      result match {
        case Left(errs) => {
          errs foreach { err => c.error(c.enclosingPosition, s"Error analyzing FieldSelector ${show(fun.tree)}: " + err) }
          reify { throw new RuntimeException("Invalid key selector."); }
        }
        case Right((udtDesc, sels)) => {
          val descs: List[Option[UDTDescriptor]] = sels flatMap { sel: List[String] => udtDesc.select(sel) }
          descs foreach { desc => desc map { desc => if (!desc.canBeKey) c.error(c.enclosingPosition, "Type " + desc.tpe + " cannot be key.") } }
          val ids = descs map { _ map { _.id } }
          ids forall { _.isDefined } match {
            case false => {
              c.error(c.enclosingPosition, s"Could not determine ids of key fields: ${ids}")
              reify { throw new RuntimeException("Invalid key selector."); }
            }
            case true => {
              val generatedIds = ids map { _.get } map { id => Literal(Constant(id: Int)) }
              val generatedList = mkList(generatedIds)
              reify {
                val list = c.Expr[List[Int]](generatedList).splice
                list
              }
            }
          }
        }
      }
      
    }

    private def getSelector(tree: Tree): Either[List[String], List[List[String]]] = tree match {

      case Function(List(p), body) => getSelector(body, Map(p.symbol -> Nil)) match {
        case err @ Left(_) => err
        case Right(sels) => Right(sels map { sel => p.name.toString +: sel })
      }

      case _ => Left(List("expected lambda expression literal but found " + show(tree)))
    }

    private def getSelector(tree: Tree, roots: Map[Symbol, List[String]]): Either[List[String], List[List[String]]] = tree match {

      case SimpleMatch(body, bindings) => getSelector(body, roots ++ bindings)

      case Match(_, List(CaseDef(pat, EmptyTree, _))) => Left(List("case pattern is too complex"))
      case Match(_, List(CaseDef(_, guard, _))) => Left(List("case pattern is guarded"))
      case Match(_, _ :: _ :: _) => Left(List("match contains more than one case"))

      case TupleCtor(args) => {

        val (errs, sels) = args.map(arg => getSelector(arg, roots)).partition(_.isLeft)

        errs match {
          case Nil => Right(sels.map(_.right.get).flatten)
          case _ => Left(errs.map(_.left.get).flatten)
        }
      }

      case Apply(tpt@TypeApply(_, _), _) => Left(List("constructor call on non-tuple type " + tpt.tpe))

      case Ident(name) => roots.get(tree.symbol) match {
        case Some(sel) => Right(List(sel))
        case None => Left(List("unexpected identifier " + name))
      }

      case Select(src, member) => getSelector(src, roots) match {
        case err @ Left(_) => err
        case Right(List(sel)) => Right(List(sel :+ member.toString))
        case _ => Left(List("unsupported selection"))
      }

      case _ => Left(List("unsupported construct of kind " + showRaw(tree)))

    }

    private object SimpleMatch {

      def unapply(tree: Tree): Option[(Tree, Map[Symbol, List[String]])] = tree match {

        case Match(arg, List(cd @ CaseDef(CasePattern(bindings), EmptyTree, body))) => Some((body, bindings))
        case _ => None
      }

      private object CasePattern {

        def unapply(tree: Tree): Option[Map[Symbol, List[String]]] = tree match {

          case Apply(MethodTypeTree(params), binds) => {
            val exprs = params.zip(binds) map {
              case (p, CasePattern(inners)) => Some(inners map { case (sym, path) => (sym, p.name.toString +: path) })
              case _ => None
            }
            if (exprs.forall(_.isDefined)) {
              Some(exprs.flatten.flatten.toMap)
            }
            else
              None
          }

          case Ident(_) | Bind(_, Ident(_)) => Some(Map(tree.symbol -> Nil))
          case Bind(_, CasePattern(inners)) => Some(inners + (tree.symbol -> Nil))
          case _ => None
        }
      }

      private object MethodTypeTree {
        def unapply(tree: Tree): Option[List[Symbol]] = tree match {
          case _: TypeTree => tree.tpe match {
            case MethodType(params, _) => Some(params)
            case _ => None
          }
          case _ => None
        }
      }
    }

    private object TupleCtor {

      def unapply(tree: Tree): Option[List[Tree]] = tree match {
        case Apply(tpt@TypeApply(_, _), args) if isTupleTpe(tpt.tpe) => Some(args)
        case _ => None
      }

      private def isTupleTpe(tpe: Type): Boolean = definitions.TupleClass.contains(tpe.typeSymbol)
    }
  }

  protected def chkSelectors(udt: UDTDescriptor, sels: List[List[String]]): List[String] = {
    sels flatMap { sel => chkSelector(udt, sel.head, sel.tail) }
  }

  protected def chkSelector(udt: UDTDescriptor, path: String, sel: List[String]): Option[String] = (udt, sel) match {
    case (_, Nil) if udt.isPrimitiveProduct => None
    case (_, Nil) => Some(path + ": " + udt.tpe + " is not a primitive or product of primitives")
    case (_, field :: rest) => udt.select(field) match {
      case None => Some("member " + field + " is not a case accessor of " + path + ": " + udt.tpe)
      case Some(udt) => chkSelector(udt, path + "." + field, rest)
    }
  }

}