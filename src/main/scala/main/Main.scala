package main

import scala.collection.mutable
import java.io._
import scala.util.parsing.combinator.PackratParsers
import relation.RelAlgParser
import relation.Scope

// TODO Outher Joins

object Main {
    def time[T](fun: => T) = {
        val time = System.currentTimeMillis()
        val returnVal = fun
        println(System.currentTimeMillis() - time)
        returnVal
    }
    
    def main(args: Array[String]) {
        val parser = new RelAlgParser(Scope.fromDir(new File(Properties("schema_folder"))))
        
        try {
        	val res = parser.eval("""
        	    rename|S(pid, n, gebdat)| (
        	        select|patientId<=3 and patientId >=2|patient
        	    ) [X]|S.pid=patientMedikament.patient| patientMedikament
        	""")
        	res.prettyPrint
        } catch {
            case re: RuntimeException => {
            	println(re.getMessage())
                // re.printStackTrace()
            }
        }
    }
	
    def eval(parser: RelAlgParser, reader: scala.util.parsing.input.StreamReader) = {
        parser.relation(new parser.PackratReader(new parser.lexical.Scanner(reader))) match {
        	case parser.Success(rel, _) => rel
        	case parser.Failure(msg, _) => throw new RuntimeException(msg)
        	case parser.Error(msg, _) => throw new RuntimeException(msg)
        }
    }
}


