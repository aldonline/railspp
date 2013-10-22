package etl

import java.sql.ResultSet

object misc {
  def optNull[T]( v:T ):Option[T] = if (v == null) None else Some(v)
}

object values {
  // TODO: inherit from AnyVal
  trait Value
  case class Str( val v:String ) extends Value
  case class IntV( val v:Int ) extends Value
  case class Id( val v:String ) extends Value
  object Id {
    def apply( tableName:String, local:String ):Id = Id( tableName + "/" + local )    
  }
  
  case class Time( val v:String ) extends Value
  case class Bool( val v:Boolean ) extends Value
  case class FloatV( val v:Float ) extends Value
  case class TimeSt( val v:java.sql.Timestamp ) extends Value
  case class Fact( s:Value, p:Id, o:Value )
}

// pimp my library
object pimp {
  import values._
  import scala.collection.JavaConversions._
  import net.sourceforge.schemaspy.model._

  type RS = ResultSet

  implicit def c2c( col:TableColumn ) = new TableColumn2(col)
  class TableColumn2(col:TableColumn){

    def getParents2:List[TableColumn] =
      for {
        k <- col.getTable.getForeignKeys.toList
        if ( k.getChildColumns.exists( _.getName == col.getName ))
        p <- k.getParentColumns
      } yield { p }
          
          
    def factExtractor: (Id, RS) => Option[Fact] = {
      val predicate = Id( col.getTable.getName + "." + col.getName )
      val fe = col.fieldExtractor
      (id:Id, rs:RS) => fe(rs).map( Fact( id, predicate, _ ) )
    }
    
    def fieldExtractor: RS => Option[Value] = {
      val colname = col.getName
      val parents = col.getParents2
      if ( parents.size > 1 )
        throw new Error("more than one parent?" + col.getName + " > " + parents.map(_.getName).mkString(" "))
      if ( parents.size > 0 ){ // reference. the value is a foreign idCol
        val pcol = parents.head
        val pcolname = pcol.getName
        val ptablename = pcol.getTable.getName
        return (rs:RS) => misc.optNull( rs getString colname ).map( Id( ptablename, _ ) )
      }
      
      def cc[T]( cast:(RS,String) => T, wrap:(T) => values.Value ) = (rs:RS) => { misc.optNull( cast( rs, colname) ).map( wrap ) }
      val strx = cc( (_ getString _), Str )
      
      col.getType match {
        // serial, int4, varchar, bool, timestamp, text, float8
        case "serial"    => strx
        case "int4"      => cc( (_ getInt _), IntV )
        case "varchar"   => strx
        case "bool"      => cc( (_ getBoolean _), Bool ) 
        case "timestamp" => cc( (_ getTimestamp _), TimeSt )
        case "text"      => strx
        case "float8"    => cc( (_ getFloat _), FloatV )
      }
    }  
  }
  
  
  sealed trait TableType
  case object EntityTable extends TableType
  case object RelationTable extends TableType
  
  implicit def t2t( table:Table ) = new Table2(table)
  class Table2( table:Table ){
    
    def getType:Option[TableType] =
      if ( table.getColumn("id") != null )
        Some(EntityTable)  
      else if ( table.getColumns.size == 2 )     
        Some(RelationTable)
      else
        None
    
    protected def entityRowExtractor: RS => List[Fact] = {
      val idExtractor = table.idExtractor
      val valueFactExtractors = table.getColumns.filter(_.getName != "id").map( _.factExtractor)
      (rs:RS) => {
        val id = idExtractor(rs)
        valueFactExtractors.flatMap( _( id, rs )).toList
      }
    }
    
    protected def relationRowExtractor: RS => List[Fact] = {
      val predicate = Id( table.getName )
      val e = table.getColumns.map(_.fieldExtractor).toList
      val f = (a:Value, b:Value) => Fact( a, predicate, b )
      (rs:RS) => ( for { a <- e(0)(rs) ; b <- e(1)(rs) } yield f(a, b) ).toList
    }
    
    def rowExtractor:Option[RS => List[Fact]] = table.getType map {
      case EntityTable => table.entityRowExtractor
      case RelationTable => table.relationRowExtractor
    }

        
    protected def idExtractor: RS => Id = {
      val tname = table.getName
      (rs:RS) => Id( tname, rs getString "id" )
    }
  
  }  
}

object Processor {
  
  type RS = ResultSet
  type Fetcher = (String) => Stream[RS]
  
  import values._
  import scala.collection.JavaConversions._
  import net.sourceforge.schemaspy.model._
  import pimp._
  
  def facts( db:Database, f:Fetcher ):Stream[Fact] = db.getTables.toStream.flatMap(facts(_, f))
  
  def facts( table:Table, f:Fetcher  ):Stream[Fact] = table.rowExtractor match {
    case None => Nil.toStream
    case Some(e) => f("SELECT * FROM " + table) flatMap e
  }
}

object Util {
  
  def test =  Processor.facts(db.spyDB, db.sql _ )    
  
  def testWithTimer = {
    val start = System.nanoTime  
    println("start...") 
    println( "size = " + test.toStream.size )   
    println("end. ms = " + ( System.nanoTime - start ))    
  }
  
  lazy val db = new _root_.util.PGDB( host = "localhost", user = "abucchi", dbname = "cbcprod" )
  
}
