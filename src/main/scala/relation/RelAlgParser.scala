package relation

import scala.util.parsing.combinator.syntactical._
import scala.util.parsing.combinator.PackratParsers
import java.io.StringReader

case class Lexem[+T](val value: T)

class RelAlgParser(scope: Scope) extends StandardTokenParsers with PackratParsers {
    import lexical.{Keyword, NumericLit, StringLit, Identifier}
    
    lexical.delimiters ++= List("(", ")", "|", ",", "<=", "<", ">=", ">", "!=", "=", "*", ".", "+", "-", "*", "/")
    lexical.reserved += ("select", "project", "rename", "aggregate", "and", "or", "not", "intersect", "union", "except", "cross", "join", "inner", "leftsemi", "rightsemi", "leftouter", "rightouter", "fullouter", "null")

    def eval(query: String) = {
        val inReader = scala.util.parsing.input.StreamReader(new StringReader(query))
        
        relationPhrase(new PackratReader(new lexical.Scanner(inReader))) match {
        	case Success(rel, _) => rel
        	case Failure(msg, _) => throw new RuntimeException(msg)
        	case Error(msg, _) => throw new RuntimeException(msg)
        }
    }
    
    lazy val relationPhrase: PackratParser[Relation] = phrase(relation)
    
    lazy val relation: PackratParser[Relation] = 
        relation2 ~ (
            setOperatorSymbol ~ (
                relation | 
                error("missing relation after set operator")
            ) |
            error("missing or invalid set operator after relation")
        ).? ^^ {
        case (rel: Relation) ~ Some(operator ~ (rel2: Relation)) => {
            operator match {
                case "intersect" => rel.intersect(rel2)
                case "union" => rel.union(rel2)
                case "except" => rel.except(rel2)
                case "cross" => rel.crossProduct(rel2)
            }
        }
        case (rel: Relation) ~ None => rel
    }
    
    lazy val setOperatorSymbol: PackratParser[String] = 
        ("intersect" | "union" | "except" | "cross") ^^ {
        case "intersect" => "intersect"
        case "union" => "union"
        case "except" => "except"
        case "cross" => "cross"
    }
        
    def attNameList2Cond(attNameList: List[String], rel1: Relation, rel2: Relation) = (relationHead: RelationHead, tuple: Map[Attribute, Any]) => 
        attNameList.forall(attName => tuple(rel1.attribute(attName)) == tuple(rel2.attribute(attName)))
    
    lazy val relation2: PackratParser[Relation] = 
        relation3 ~ (
            "join" ~ "(" ~ joinType ~ ")" ~ (
                (cond | attList) ~ relation | 
                error("missing join condition or relation after join symbol")
            )
        ).? ^^ {
        case rel1 ~ Some(
	            "join" ~ "(" ~ joinType ~ ")" ~ (
	                (cond: ((RelationHead, Map[Attribute, Any]) => Boolean)) ~ rel2
	            )) => joinType match {
                    case "inner" => rel1.join(rel2, cond)
                    case "leftsemi" => rel1.leftSemiJoin(rel2, cond)
                    case "rightsemi" => rel1.rightSemiJoin(rel2, cond)
                    case "leftouter" => rel1.leftOuterJoin(rel2, cond)
                    case "rightouter" => rel1.rightOuterJoin(rel2, cond)
                    case "fullouter" => rel1.fullOuterJoin(rel2, cond)
                }
        case rel1 ~ Some(
	            "join" ~ "(" ~ joinType ~ ")" ~ (
	                (attNameList: List[String]) ~ rel2
	            )) => joinType match {
                	case "inner" => rel1.join(rel2, attNameList2Cond(attNameList, rel1, rel2))
                    case "leftsemi" => rel1.leftSemiJoin(rel2, attNameList2Cond(attNameList, rel1, rel2))
                    case "rightsemi" => rel1.rightSemiJoin(rel2, attNameList2Cond(attNameList, rel1, rel2))
                    case "leftouter" => rel1.leftOuterJoin(rel2, attNameList2Cond(attNameList, rel1, rel2))
                    case "rightouter" => rel1.rightOuterJoin(rel2, attNameList2Cond(attNameList, rel1, rel2))
                    case "fullouter" => rel1.fullOuterJoin(rel2, attNameList2Cond(attNameList, rel1, rel2))
                }
	    case rel1 ~ None => rel1
    }
    
    lazy val joinType: PackratParser[String] = 
        ("inner" | "leftsemi" | "rightsemi" | "leftouter" | "rightouter" | "fullouter") ^^ {
        case "inner" => "inner"
        case "leftsemi" => "leftsemi"
        case "rightsemi" => "rightsemi"
        case "leftouter" => "leftouter"
        case "rightouter" => "rightouter"
        case "fullouter" => "fullouter"
    }
        
    lazy val relation3: PackratParser[Relation] = 
        (
            "aggregate" ~ (aggFun ~ relation3 | error("Invalid or missing aggregation function or relation after \"F\"")) | 
            attList ~ "aggregate" ~ (aggFun ~ relation3 | error("Invalid or missing aggregation function or relation after \"F\"")) |
            relation4
        ) ^^ {
        case (relation: Relation) => relation
        case 
            (attList: List[String]) ~ 
            "aggregate" ~ 
            ((aggFun: ((Relation, Set[Map[Attribute, Any]]) => Any)) ~ 
            (relation: Relation))
                => relation.aggregate(attList, aggFun(relation, _))
        case 
            "aggregate" ~ 
            ((aggFun: ((Relation, Set[Map[Attribute, Any]]) => Any)) ~ 
            (relation: Relation))
                => relation.aggregate(aggFun(relation, _))
    }
    
    lazy val aggFun: PackratParser[(Relation, Set[Map[Attribute, Any]]) => Any] =
        "|" ~ aggFun2 ~ "|" ^^ {
        case "|" ~ aggFun2 ~ "|" => aggFun2
    }
    
    lazy val aggFun2: PackratParser[(Relation, Set[Map[Attribute, Any]]) => Any] = 
        (
            ident ~ "(" ~ ident ~ ")" | 
            ident ~ "(" ~ "*" ~ ")" 
        ) ^^ {
        case funName ~ "(" ~ "*" ~ ")" => (relation: Relation, group: Set[Map[Attribute, Any]]) => group.size
        
        case funName ~ "(" ~ attName ~ ")" => funName match {
            case "sum" => (
                (relation: Relation, group: Set[Map[Attribute, Any]]) => 
                    (0 /: group)(
                        (old, tuple) => old + tuple(relation.attribute(attName)).asInstanceOf[Int]
                    )
            )
            case "count" => (relation: Relation, group: Set[Map[Attribute, Any]]) => 
                (0 /: group)(
                    (old, tuple) => old + 
	                    (if(
	                        tuple.getOrElse(relation.attribute(attName), None) != None && 
	                        tuple.getOrElse(relation.attribute(attName), None) != null
	                    ) 1 else 0)
                )
            case "max" => (relation: Relation, group: Set[Map[Attribute, Any]]) => 
                group.map(tuple => tuple(relation.attribute(attName)).asInstanceOf[Int]).max
            case "min" => (relation: Relation, group: Set[Map[Attribute, Any]]) => 
                group.map(tuple => tuple(relation.attribute(attName)).asInstanceOf[Int]).min
            case _ => throw new RuntimeException()
        }
    }
        
    lazy val relation4: PackratParser[Relation] =
        (
            "select" ~ (cond ~ relation3 | error("Invalid or missing condition or relation after selection")) | 
            "project" ~ (attList ~ relation3 | error("Invalid or missing attribute list or relation after projection")) | 
            "rename" ~ ("|" ~ ident.? ~ ("(" ~> rep1sep(ident, ",") <~ ")").? ~ "|" ~ relation3 | error("Invalid or missing attribute list or relation after rename")) |
            "(" ~ relation ~ ")" |
            ident
            
        ) ^^ { 
        	case id: String => scope(id)
        	case "select" ~ ((cond: ((RelationHead, Map[Attribute, Any]) => Boolean)) ~ (relation: Relation)) => 
        	    relation.select(cond(relation.relationHead, _))
        	case "project" ~ (attList ~ (relation: Relation)) if attList.isInstanceOf[List[String]]=> 
        	    relation.project(attList.asInstanceOf[List[String]])
        	case "rename" ~ ("|" ~ (newRelName: Option[String]) ~ (atts: Option[List[String]]) ~ "|" ~ (relation: Relation)) if atts.isInstanceOf[Option[List[String]]] => 
        	    relation.rename(newRelName, atts)
        	case "(" ~ (relation: Relation) ~ ")" => relation
        }
    
    lazy val attList: PackratParser[List[String]] = 
        "|" ~> rep1sep(qualifiedId, ",") <~ "|" ^^ {
        case attList => attList
    }
    
    lazy val cond: PackratParser[(RelationHead, Map[Attribute, Any]) => Boolean] = 
        "|" ~ disjuncTerm ~ "|" ^^ {
        case "|" ~ disjuncTerm ~ "|" => disjuncTerm
    }
        
    lazy val disjuncTerm: PackratParser[(RelationHead, Map[Attribute, Any]) => Boolean] = 
        rep1sep(conjuncTerm, "or") ^^ {
        case conjuncTerms => conjuncTerms.reduceLeft((oldFun, term) => ((rel: RelationHead, tuple: Map[Attribute, Any]) => oldFun(rel, tuple) || term(rel, tuple)))
    }

    lazy val conjuncTerm: PackratParser[(RelationHead, Map[Attribute, Any]) => Boolean] = 
        rep1sep(relationTerm, "and") ^^ {
        case negateTerms => negateTerms.reduceLeft((oldFun, term) => ((rel: RelationHead, tuple: Map[Attribute, Any]) => oldFun(rel, tuple) && term(rel, tuple)))
    }
        
    def relSign2Fun(sign: String) = (arg1: Any, arg2: Any) => sign match {
        case "=" => arg1 == arg2
        case "!=" => arg1 != arg2
        case "<" => arg1 match {
            case i: Integer => arg2 match {
                case j: Integer => i < j
                case _ => throw new RuntimeException("lexem \"" + arg2 + "\" found in '<' relation which its no Int")
            }
            case _ => throw new RuntimeException("lexem \"" + arg1 + "\" found in '<' relation which its no Int")
        }
        case "<=" => arg1 match {
            case i: Integer => arg2 match {
                case j: Integer => i <= j
                case _ => throw new RuntimeException("lexem \"" + arg2 + "\" found in '<=' relation which its no Int")
            }
            case _ => throw new RuntimeException("lexem \"" + arg1 + "\" found in '<=' relation which its no Int")
        }
        case ">" => arg1 match {
            case i: Integer => arg2 match {
                case j: Integer => i > j
                case _ => throw new RuntimeException("lexem \"" + arg2 + "\" found in '>' relation which its no Int")
            }
            case _ => throw new RuntimeException("lexem \"" + arg1 + "\" found in '>' relation which its no Int")
        }
        case ">=" => arg1 match {
            case i: Integer => arg2 match {
                case j: Integer => i >= j
                case _ => throw new RuntimeException("lexem \"" + arg2 + "\" found in '>=' relation which its no Int")
            }
            case _ => throw new RuntimeException("lexem \"" + arg1 + "\" found in '>=' relation which its no Int")
        }
        case _ => throw new RuntimeException("Relation " + rel + " is not valid")
    }
    
    
    lazy val relationTerm: PackratParser[(RelationHead, Map[Attribute, Any]) => Boolean] = 
        (
            negateTerm | 
            addSubstractTerm ~ rel ~ addSubstractTerm
        ) ^^ {
        case negateFun: ((RelationHead, Map[Attribute, Any]) => Boolean) => negateFun
        case (term1: ((RelationHead, Map[Attribute, Any]) => Any)) ~ 
             (relSign: String) ~ 
             (term2: ((RelationHead, Map[Attribute, Any]) => Any)) => 
                 (head: RelationHead, tuple: Map[Attribute, Any]) => 
                     relSign2Fun(relSign)(term1(head, tuple), term2(head, tuple))
    }
    
    lazy val negateTerm: PackratParser[(RelationHead, Map[Attribute, Any]) => Boolean] = 
        (
            "not" ~ "(" ~ disjuncTerm ~ ")" | 
            "(" ~ disjuncTerm ~ ")"
        ) ^^ {
        case "not" ~ "(" ~ (disjuncFun: ((RelationHead, Map[Attribute, Any]) => Boolean)) ~ ")" => ((rel: RelationHead, tuple: Map[Attribute, Any]) => if(disjuncFun(rel, tuple)) false else true)
        case "(" ~ (disjuncFun: ((RelationHead, Map[Attribute, Any]) => Boolean)) ~ ")" => disjuncFun
    }
    
    lazy val addSubstractTerm: PackratParser[(RelationHead, Map[Attribute, Any]) => Any] = 
        devideMultiplicationTerm ~ (("+" | "-") ~ devideMultiplicationTerm).? ^^ {
        case term ~ (Some("+" ~ term2)) => (head: RelationHead, tuple: Map[Attribute, Any]) => term(head, tuple).asInstanceOf[Int] + term2(head, tuple).asInstanceOf[Int]
        case term ~ (Some("-" ~ term2)) => (head: RelationHead, tuple: Map[Attribute, Any]) => term(head, tuple).asInstanceOf[Int] - term2(head, tuple).asInstanceOf[Int]
        case term ~ None => term
    }
    
    lazy val devideMultiplicationTerm: PackratParser[(RelationHead, Map[Attribute, Any]) => Any] = 
        mathNegateTerm ~ (("*" | "/") ~ mathNegateTerm).? ^^ {
        case term ~ (Some("*" ~ term2)) => (head: RelationHead, tuple: Map[Attribute, Any]) => term(head, tuple).asInstanceOf[Int] * term2(head, tuple).asInstanceOf[Int]
        case term ~ (Some("/" ~ term2)) => (head: RelationHead, tuple: Map[Attribute, Any]) => term(head, tuple).asInstanceOf[Int] / term2(head, tuple).asInstanceOf[Int]
        case term ~ None => term
    }
        
    lazy val mathNegateTerm: PackratParser[(RelationHead, Map[Attribute, Any]) => Any] =
        ("-").? ~ mathAtom ^^ {
        case Some("-") ~ mathAtom => (head: RelationHead, tuple: Map[Attribute, Any]) => -mathAtom(head, tuple).asInstanceOf[Int]
        case None ~ mathAtom => mathAtom
    }
    
    lazy val mathAtom: PackratParser[(RelationHead, Map[Attribute, Any]) => Any] =
        (
            "null" | 
            qualifiedId | 
            lexem | 
            "(" ~> addSubstractTerm <~ ")" |
            "(" ~> relation <~ ")"
        ) ^^ {
        case "null" => ((_: RelationHead, _: Map[Attribute, Any]) => null)
        case id: String => ((head: RelationHead, tuple: Map[Attribute, Any]) => tuple(head.attribute(id)))
        case lexem: Lexem[_] => ((_: RelationHead, _: Map[Attribute, Any]) => lexem.value)
        case bracedTerm: ((RelationHead, Map[Attribute, Any]) => Any) => bracedTerm
        case relation: Relation => {
            if(relation.content.size == 1 && relation.content.toList(0).size == 1) {
                val returnValue = (for(value <- relation.content.toList(0)) yield value._2).toList(0)
                (head: RelationHead, tuple: Map[Attribute, Any]) => returnValue
            } else {
                throw new RuntimeException("Relation must be atomic to treat it as a number")
            }
        }
    }
        
    lazy val qualifiedId: PackratParser[String] = 
        rep1sep(ident, ".") ^^ {
        list => list.mkString(".")
    }
    
    lazy val rel: PackratParser[String] = 
        ("<=" | "<" | ">=" | ">" | "=" | "!=") ^^ {
        case "<=" => "<="
        case "<" => "<"
        case ">=" => ">="
        case ">" => ">"
        case "=" => "="
        case "!=" => "!="
    }
        
    lazy val lexem: PackratParser[Lexem[Any]] = 
        (intLit | stringLit) ^^ {
        case intLit: Int => new Lexem[Int](intLit)
        case stringLit: String => new Lexem[String](stringLit)
    }
    
    lazy val intLit: PackratParser[Int] = 
    	elem("number", _.isInstanceOf[NumericLit]) ^^ (_.chars.toInt)
}