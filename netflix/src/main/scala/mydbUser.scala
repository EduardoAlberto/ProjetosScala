import java.util.Properties

object mydbUser {
  val properties = new Properties()
  properties.put("user", "postgres")
  properties.put("password", "postgre123")

  val url = "jdbc:postgresql://localhost:5432/dev5114"
}
