import org.junit.Test;
import redis.clients.jedis.Jedis;

public class jedisTest {
    @Test
    public void demo(){
        Jedis jedis =new Jedis("hadoop002",6379);
        jedis.auth("!QE%^E3323BDWEwwwe1839");

        // 2. 保存数据
        jedis.set("name","imooc");
        // 3. 获取数据
        String value = jedis.get("name");
        System.out.println(value);
        // 4.释放资源
        jedis.close();

    }

}
