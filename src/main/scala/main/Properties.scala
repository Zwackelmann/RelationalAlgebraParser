package main

import java.io.FileInputStream
import java.util.{Properties => JavaProperties}

object Properties {
    val is = new FileInputStream("relalg.properties")
    val properties = new JavaProperties()
    properties.load(is)
        
	def apply(property: String) = properties.getProperty(property)
}