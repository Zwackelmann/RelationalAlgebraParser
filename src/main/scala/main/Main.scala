package main

import scala.collection.mutable
import java.io._
import scala.util.parsing.combinator.PackratParsers
import relation.RelAlgParser
import relation.Scope

object Main {
    def time[T](fun: => T) = {
        val time = System.currentTimeMillis()
        val returnVal = fun
        println(System.currentTimeMillis() - time)
        returnVal
    }
    
    def main(args: Array[String]) {
        val parser = new RelAlgParser(Scope.fromDir(new File("data/" + Properties("schema_folder"))))
        
        try {
        	val res = parser.eval("""
        	    patient  except  (select |patientId<=3| patient)
        	""")
        	println("here is the result: ")
        	println(res.prettyPrint)
        } catch {
            case re: RuntimeException => {
            	println(re.getMessage())
                // re.printStackTrace()
            }
        }
    }
}


