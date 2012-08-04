package relation

object Attribute {
    def apply(name: String, origin: Option[String]) = new Attribute(name, origin)
}

class Attribute(val name: String, val origin: Option[String]) {
    override def toString = "Attribute(" + name + ")"
}

class RelationHead(val atts: List[Attribute]) {
    def attribute(qualifiedAttName: String) = {
        val parts = qualifiedAttName.split("\\.").toList
        
        val (attName, origin: Option[String]) = if(parts.length > 2) {
            throw new IllegalArgumentException("Qualified attribute name must not have more than one dot")
        } else if(parts.length == 2) {
            (parts(1), Some(parts(0)))
        } else if(parts.length == 1) {
            (parts(0), None)
        }
        val res = if(origin == None) {
            atts.filter(att => att.name == attName)
        } else {
            atts.filter(att => att.name == attName && (att.origin == None || att.origin == origin))
        }
        
        if(res.size == 1) {
            res(0)
        } else if(res.size == 0) {
            throw new RuntimeException("attribute " + qualifiedAttName + " not found")
        } else {
            throw new RuntimeException("attribute " + qualifiedAttName + " is ambigous")
        }
    }
    
    def attribute(att: Attribute) = (if(atts.contains(att)) att else throw new RuntimeException("Attribute " + att + " does not exist"))
    
    def union(relationHead: RelationHead) = new RelationHead(atts ++ relationHead.atts)
    
    def apply(values: Set[Map[String, Any]]) = 
        values.map(tuple => tuple.map(key_value => attribute(key_value._1) -> key_value._2))
    
    def fromAttributeTuples(values: Set[Map[Attribute, Any]]) = 
        values.map(tuple => tuple.map(key_value => attribute(key_value._1) -> key_value._2))
    
    override def toString = atts.toString
}

object Relation {
    def apply(relName: String, reader: scala.util.parsing.input.StreamReader) = {
        RelationParser.relation(relName)(new RelationParser.lexical.Scanner(reader)) match {
        	case RelationParser.Success(rel, _) => rel
        	case RelationParser.Failure(msg, _) => throw new RuntimeException(msg)
        	case RelationParser.Error(msg, _) => throw new RuntimeException(msg)
        }
    }
}

class Relation(val relationHead: RelationHead, val content: Set[Map[Attribute, Any]]) {
    def attribute(attName: String) = relationHead.attribute(attName)
    
    def select(cond: (Map[Attribute, Any] => Boolean)) = 
        new Relation(relationHead, content.filter(cond))
    
    def project(attNameList: List[String]) = 
        new Relation(
            new RelationHead(attNameList.map(attribute(_))),
            content.map((tuple) => attNameList.map(attName => attribute(attName) -> tuple(attribute(attName))).toMap)
        )
    
    def rename(newRelName: Option[String], attNameList: Option[List[String]]) = {
        if(attNameList.isDefined && attNameList.get.size != relationHead.atts.size) {
            throw new IllegalArgumentException("Number of attributes for renaming does not match")
        }
        
        val newHead: RelationHead = newRelName match {
            case Some(newRelName) => attNameList match {
                case Some(attNameList) => new RelationHead(
		            attNameList.map(
		                attName => new Attribute(attName, Some(newRelName))
		            )
		        )
                case None => new RelationHead(
		            relationHead.atts.map(
		                att => new Attribute(att.name, Some(newRelName))
		            )
		        )
            }
            case None => attNameList match {
                case Some(attNameList) => new RelationHead(
		            relationHead.atts.zip(attNameList).map(
		                pair => new Attribute(pair._2, pair._1.origin)
		            )
		        )
                case None => throw new RuntimeException("rename operator has neither a new relation name nor new attribute names")
            }
        }
        
        val attMap = relationHead.atts.zip(newHead.atts).toMap
        val newContent = content.map(tuple => tuple.map(att_value => attMap(att_value._1) -> att_value._2))
        new Relation(newHead, newContent)
    }
    
    def join(relation: Relation, cond: ((RelationHead, Map[Attribute, Any]) => Boolean)) = {
        val relHead = relationHead.union(relation.relationHead)
        new Relation(
            relHead,
            relHead.fromAttributeTuples(for(
                tuple <- content; 
                tuple2 <- relation.content 
                if(cond(relHead, tuple ++ tuple2))) yield tuple ++ tuple2
            )
        )
    }

    def leftSemiJoin(relation: Relation, cond: ((RelationHead, Map[Attribute, Any]) => Boolean)) = {
        val tempRelationHead = new RelationHead(relationHead.atts ++ relation.relationHead.atts)
        new Relation(relationHead, for(tuple <- content; tuple2 <- relation.content if(cond(tempRelationHead, tuple ++ tuple2))) yield tuple)
    }
        
    def rightSemiJoin(relation: Relation, cond: ((RelationHead, Map[Attribute, Any]) => Boolean)) = {
        val tempRelationHead = new RelationHead(relationHead.atts ++ relation.relationHead.atts)
        new Relation(relation.relationHead, for(tuple <- content; tuple2 <- relation.content if(cond(tempRelationHead, tuple ++ tuple2))) yield tuple2)
    }
    
    def leftOuterJoin(relation: Relation, cond: ((RelationHead, Map[Attribute, Any]) => Boolean)) = {
        val relHead = relationHead.union(relation.relationHead)
        val newContent = (for(tuple <- content) yield {
            val joinPartnersForTuple = for(tuple2 <- relation.content if(cond(relHead, tuple ++ tuple2))) yield tuple ++ tuple2
            
            if(joinPartnersForTuple.isEmpty) {
                List(tuple ++ (relation.relationHead.atts.map(att => att -> null)).toMap)
            } else {
                joinPartnersForTuple
            }
        }).flatten
        
        new Relation(
            relHead, newContent
        )
    }
    
    def rightOuterJoin(relation: Relation, cond: ((RelationHead, Map[Attribute, Any]) => Boolean)) = {
        val relHead = relationHead.union(relation.relationHead)
        val newContent = (for(tuple <- relation.content) yield {
            val joinPartnersForTuple = for(tuple2 <- content if(cond(relHead, tuple ++ tuple2))) yield tuple ++ tuple2
            
            if(joinPartnersForTuple.isEmpty) {
                List(tuple ++ (relationHead.atts.map(att => att -> null)).toMap)
            } else {
                joinPartnersForTuple
            }
        }).flatten
        
        new Relation(
            relHead, newContent
        )
    }
    
    def fullOuterJoin(relation: Relation, cond: ((RelationHead, Map[Attribute, Any]) => Boolean)) = {
        val relHead = relationHead.union(relation.relationHead)
        val newContentLeftOuterJoin = (for(tuple <- content) yield {
            val joinPartnersForTuple = for(tuple2 <- relation.content if(cond(relHead, tuple ++ tuple2))) yield tuple ++ tuple2
            
            if(joinPartnersForTuple.isEmpty) {
                List(tuple ++ (relation.relationHead.atts.map(att => att -> null)).toMap)
            } else {
                joinPartnersForTuple
            }
        }).flatten
        
        val missingTuplesFromRightRelation = (for(tuple <- relation.content) yield {
            val joinPartnersForTuple = for(tuple2 <- content if(cond(relHead, tuple ++ tuple2))) yield tuple ++ tuple2
            
            if(joinPartnersForTuple.isEmpty) {
                List(List(tuple ++ (relationHead.atts.map(att => att -> null)).toMap))
            } else {
                List()
            }
        }).flatten.flatten
        
        new Relation(
            relHead, (newContentLeftOuterJoin ++ missingTuplesFromRightRelation)
        )
    }
    
    def aggregate(attNameList: List[String], aggFun: (Set[Map[Attribute, Any]] => Any)) = {
        val aggregatedColAttribute = Attribute("col_" + (attNameList.size + 1), None)    
        val attList = attNameList.map(attribute(_))        
        
        val relationHead = new RelationHead(attList :+ aggregatedColAttribute)
        new Relation(
            relationHead,
            relationHead.fromAttributeTuples(
                content
	                .groupBy(tuple => attList.map(att => att -> tuple(att)).toMap)
	                .map((groupAtts_group) => 
	                    (groupAtts_group._1 + (aggregatedColAttribute -> aggFun(groupAtts_group._2)))
	                )
	                .toSet
            )
        )
    }
    
    def aggregate(aggFun: (Set[Map[Attribute, Any]] => Any)) = {
        val aggregatedColName = "col_0"
        
        val relationHead = new RelationHead(List(Attribute(aggregatedColName, None)))
        new Relation(
            relationHead,
            relationHead(Set(Map(aggregatedColName -> aggFun(content))))
        )
    }
    
    private def setOperator(
        rel: Relation, 
        setOperation: (Set[Map[Attribute, Any]], Set[Map[Attribute, Any]]) => Set[Map[Attribute, Any]]
    ) = {
        if(rel.relationHead.atts.size != relationHead.atts.size) {
            throw new IllegalArgumentException("Number of arguments for set operator does not match")
        }
        
        val numAttributes = relationHead.atts.size
        val newRelationHead = new RelationHead((for(i <- 0 until numAttributes) yield new Attribute("col_" + i, None)).toList)
        val thisRelMapping = relationHead.atts.zip(newRelationHead.atts).toMap
        val otherRelMapping = rel.relationHead.atts.zip(newRelationHead.atts).toMap
        
        val newContent = (
            setOperation((for(tuple <- content) yield (tuple.map(att_value => thisRelMapping(att_value._1) -> att_value._2))),
            (for(tuple <- rel.content) yield (tuple.map(att_value => otherRelMapping(att_value._1) -> att_value._2))))
        )
        
        new Relation(newRelationHead, newContent)
    }
    
    def intersect(rel: Relation) = setOperator(rel, (a: Set[Map[Attribute, Any]], b: Set[Map[Attribute, Any]]) => a intersect b)
    def union(rel: Relation) = setOperator(rel, (a: Set[Map[Attribute, Any]], b: Set[Map[Attribute, Any]]) => a union b)
    def except(rel: Relation) = setOperator(rel, (a: Set[Map[Attribute, Any]], b: Set[Map[Attribute, Any]]) => a -- b)
    
    def crossProduct(rel: Relation) = {
        val thisRelHeaderMapping = relationHead.atts.map(att => att -> new Attribute(att.name, att.origin)).toMap
        val otherRelHeaderMapping = rel.relationHead.atts.map(att => att -> new Attribute(att.name, att.origin)).toMap
        
        val newRelationHead = new RelationHead(thisRelHeaderMapping.map(_._2).toList ++ otherRelHeaderMapping.map(_._2).toList)
        
        val newContent = for(thisRelTuple <- content; otherRelTuple <- rel.content) yield (
            thisRelTuple.map(value => thisRelHeaderMapping(value._1) -> value._2) ++ 
            otherRelTuple.map(value => otherRelHeaderMapping(value._1) -> value._2)
        )
        new Relation(newRelationHead, newContent)
    }
    
    def unbindTuple(tuple: Map[Attribute, Any]) = tuple.map(att_value => att_value._1.name -> att_value._2)
        
    def prettyPrint = {
        val sb = new StringBuffer
        
        def calulateAttributeWidth(att: Attribute) = 
        	(content.map(tuple => (if(tuple(att) == null) "null" else tuple(att).toString()).length) + att.name.length).max
        
        val attributeWidths = relationHead.atts.map(att => att -> calulateAttributeWidth(att)).toMap
        
        val totalWidth = ((0 /: attributeWidths)((old, attWidth) => old + attWidth._2)) + (3*attributeWidths.size) + 1
        
        for(att <- relationHead.atts) {
            sb.append(("| %-" + attributeWidths(att) + "s ").format(att.name))
        }
        sb.append("|\n")
        sb.append("-" * totalWidth)
        sb.append("\n")
        
        for(tuple <- content) {
            for(att <- relationHead.atts) {
                val align = if(tuple(att).isInstanceOf[Int]) "" else "-"
                sb.append(("| %" + align + attributeWidths(att) + "s ").format(tuple(att)))
            }
            sb.append("|\n")
        }
        
        sb.toString
    }
    
    override def toString = "Relation(" + content.toString + ")"
}






