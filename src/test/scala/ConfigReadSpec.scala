import com.typesafe.config.ConfigFactory
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should
import org.scalatest.matchers.should.Matchers

class ConfigReadSpec extends AnyFlatSpec with should.Matchers{
  behavior of "ReadConf"
  it should "work" in {
    val kafkaconfig = ConfigFactory.load().getConfig("kafka")
    val kafkatopic:String = kafkaconfig.getString("TOPIC")
    kafkatopic shouldBe("TwitterData2")
  }
}
