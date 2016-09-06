//package test
//
//import io.getquill.MirrorContext
//import io.getquill.MirrorIdiom
//import io.getquill.SnakeCase
//import io.getquill.context.mirror.Row
//
//class PersonId(val id: Int) extends AnyVal
//
//object Test extends App {
//  
//  val ctx = new MirrorContext[MirrorIdiom, SnakeCase]
//  import ctx._
//  
//  case class Phone(prefix: String, number: String) extends Embedded
//  
//  case class Contacts(phone: Phone, email: String) extends Embedded
//  
//  case class Person(id: PersonId, name: String, age: Int, contacts: Contacts)
//  
//  val person = Person(new PersonId(0), "a", 1, Contacts(Phone("1", "2"), "b"))
//  
//  materializeQueryMeta[(Person, Int)]
//  
//  val q = quote((p: Person) => query[Person].insert(p).returning(_.id))
//  
//  println(run(q(lift(person))).extractor(Row(1)))
////  
//  println(run(query[Person].map(p => (p, 1))).extractor(Row(1, "2", 3, "4", "5", "6", 7)))
////  
////  
////  def ins[T: ClassTag : QueryMeta] = {
////    run(query[T])
////  }
////  
////  println(ins[Person].string)
//}