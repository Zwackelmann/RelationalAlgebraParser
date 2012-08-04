package relation

import scala.util.parsing.combinator.syntactical._
import scala.util.parsing.combinator.lexical.StdLexical
object Foo {
    import scala.util.parsing.input.CharArrayReader._
    
    val newLexical = new StdLexical() {
        override def whitespaceChar = elem("space char", ch => (ch <= ' ' && ch != EofCh && ch != '\n') || ch.toInt == 65279)
    }
}

object RelationParser extends StandardTokenParsers {
    override val lexical = Foo.newLexical
    import scala.util.parsing.input.CharArrayReader._
    import lexical.{Keyword, NumericLit, StringLit, Identifier}
    
    lexical.delimiters ++= List("|", "-", "\n")
    
    def relation(relationName: String): Parser[Relation] = 
        relationHead(relationName) ~ relationContent ^^ {
        case relationHead ~ relationContent => 
            new Relation(relationHead, relationContent.map(tuple => relationHead.atts.zip(tuple).toMap).toSet)
    }
    
    def relationHead(relationName: String): Parser[RelationHead] = 
        "|" ~> rep1sep(ident, "|") <~ "|" <~ "\n" <~ rep1("-") <~ "\n" ^^ {
        case atts => new RelationHead(atts.map(new Attribute(_, Some(relationName))))
    }
	
    def relationContent: Parser[List[List[Any]]] =
    	rep1(tuple)

    def tuple: Parser[List[Any]] = 
        (
            "|" ~> rep1sep(value, "|") <~ "|"  <~ newLineOrEof
        ) ^^ {
        case values => values
    }
    
    def newLineOrEof: Parser[Any] = 
        acceptIf(elem => (elem == lexical.EOF || elem.chars == "\n"))(_ => "EOF or newline")
    
    def value: Parser[Any] =
        (intLit | stringLit) ^^ {
        case i: Integer => i
        case s: String => s
    }
        
    def intLit: Parser[Int] = 
    	elem("number", _.isInstanceOf[NumericLit]) ^^ (_.chars.toInt)
}






