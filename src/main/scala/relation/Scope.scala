package relation

import java.io.File
import scala.collection.mutable
import java.io.InputStreamReader
import java.io.FileInputStream
import main.Properties

object Scope {
    val relFileRelExp = """(.*)\.rel""".r
    
    def fromDir(dir: File) = {
        val relations = new mutable.HashMap[String, Relation]
        
        if(!dir.isDirectory()) {
            throw new RuntimeException("Could not find schema directory \"" + dir + "\"")
        }
        
        for(file <- dir.listFiles()) {
            file.getName() match {
                case relFileRelExp(name) => {
                    val inReader = scala.util.parsing.input.StreamReader(
                    	new InputStreamReader(new FileInputStream(file), Properties("encoding"))
                    )
                    try {
                        val rel = Relation(name, inReader)
                        relations += (name -> rel)
                    } catch {
                        case re: RuntimeException => println("WARNING: could not read " + name + "(" + re.getMessage() + ")")
                    }
                }
                case _ =>
            }
	        
        }
        
        new Scope(relations.toMap)
    }
}

class Scope(scope: Map[String, Relation]) {
	def apply(relationName: String) = try {
	    scope(relationName)
	} catch {
	    case nse: NoSuchElementException => throw new NoSuchElementException("Relation \"" + relationName + "\" not found") 
	}
}