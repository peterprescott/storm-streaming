package piprescott;


import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.sourcevirtues.sentiment.storm.pure.util.Cons;

/**
 * Simple Bolt that check words of incoming sentence and mark sentence with a positive score.
 * 
 * @author Adrianos Dadis
 * 
 */
public class PositiveBolt extends BaseBasicBolt {
   private static final long serialVersionUID = 1L;
   private static final Logger LOG = LoggerFactory.getLogger(PositiveBolt.class);

   private transient ObjectMapper mapper;

   @SuppressWarnings("rawtypes")
   @Override
   public void prepare(Map stormConf, TopologyContext context) {
      mapper = new ObjectMapper();
      mapper.setSerializationInclusion(Include.NON_NULL);
   }

   @Override
   public void execute(Tuple tuple, BasicOutputCollector collector) {

      try {
         ObjectNode node = (ObjectNode) mapper.readTree(tuple.getString(0));

         String[] words = node.path(Cons.MOD_TXT).asText().split(" ");
         int wordsSize = words.length;
         int positiveWordsSize = 0;
         for (String word : words) {
            if (PositiveWords.get().contains(word)) {
               positiveWordsSize++;
            }
         }

         node.put(Cons.NUM_POSITIVE, (double) positiveWordsSize / wordsSize);

         collector.emit(new Values(node.toString()));

      } catch (Exception e) {
         LOG.error("Cannot process input. Ignore it", e);
      }

   }

   @Override
   public void declareOutputFields(OutputFieldsDeclarer declarer) {
      declarer.declare(new Fields(Cons.TUPLE_VAR_MSG));
   }

   @Override
   public Map<String, Object> getComponentConfiguration() {
      return null;
   }

   private static Set<String> POSITIVE_WORDS = new HashSet<String>(Arrays.asList(new String[] {
	"absolutely", "adorable", "accepted", "acclaimed", "accomplish", "accomplishment", "achievement", "action", "active", 
	"admire", "adventure", "affirmative", "affluent", "agree", "agreeable", "amazing", "angelic", "appealing", "approve", 
	"aptitude", "attractive", "awesome", "beaming", "beautiful", "believe", "beneficial", "bliss", "bountiful", "bounty", 
	"brave", "bravo", "brilliant", "bubbly", "calm", "celebrated", "certain", "champ", "champion", "charming", "cheery", 
	"choice", "classic", "classical", "clean", "commend", "composed", "congratulation", "constant", "cool", "courageous", 
	"creative", "cute", "dazzling", "delight", "delightful", "distinguished", "divine", "earnest", "easy", "ecstatic", 
	"effective", "effervescent", "efficient", "effortless", "electrifying", "elegant", "enchanting", "encouraging", "endorsed", 
	"energetic", "energized", "engaging", "enthusiastic", "essential", "esteemed", "ethical", "excellent", "exciting", 
	"exquisite", "fabulous", "fair", "familiar", "famous", "fantastic", "favorable", "fetching", "fine", "fitting", 
	"flourishing", "fortunate", "free", "fresh", "friendly", "fun", "funny", "generous", "genius", "genuine", "giving", 
	"glamorous", "glowing", "good", "gorgeous", "graceful", "great", "green", "grin", "growing", "handsome", "happy", 
	"harmonious", "healing", "healthy", "hearty", "heavenly", "honest", "honorable", "honored", "hug", "idea", "ideal", 
	"imaginative", "imagine", "impressive", "independent", "innovate", "innovative", "instant", "instantaneous", "instinctive", 
	"intuitive", "intellectual", "intelligent", "inventive", "jovial", "joy", "jubilant", "keen", "kind", "knowing", 
	"knowledgeable", "laugh", "legendary", "light", "learned", "lively", "lovely", "lucid", "lucky", "luminous", "marvelous", 
	"masterful", "meaningful", "merit", "meritorious", "miraculous", "motivating", "moving", "natural", "nice", "novel", 
	"now", "nurturing", "nutritious", "okay", "one", "one-hundred percent", "open", "optimistic", "paradise", "perfect", 
	"phenomenal", "pleasurable", "plentiful", "pleasant", "poised", "polished", "popular", "positive", "powerful", 
	"prepared", "pretty", "principled", "productive", "progress", "prominent", "protected", "proud", "quality", "quick", 
	"quiet", "ready", "reassuring", "refined", "refreshing", "rejoice", "reliable", "remarkable", "resounding", "respected", 
	"restored", "reward", "rewarding", "right", "robust", "safe", "satisfactory", "secure", "seemly", "simple", "skilled", 
	"skillful", "smile", "soulful", "sparkling", "special", "spirited", "spiritual", "stirring", "stupendous", "stunning", 
	"success", "successful", "sunny", "super", "superb", "supporting", "surprising", "terrific", "thorough", "thrilling", 
	"thriving", "tops", "tranquil", "transforming", "transformative", "trusting", "truthful", "unreal", "unwavering", "up", 
	"upbeat", "upright", "upstanding", "valued", "vibrant", "victorious", "victory", "vigorous", "virtuous", "vital", 
	"vivacious", "wealthy", "welcome", "well", "whole", "wholesome", "willing", "wonderful", "wondrous", "worthy", 
	"wow", "yes", "yummy", "zeal", "zealous",
    }));

      static PositiveWords get() {
         if (_singleton == null) {
            synchronized (PositiveWords.class) {
               if (_singleton == null) {
                  _singleton = new PositiveWords();
               }
            }
         }

         return _singleton;
      }

      boolean contains(String key) {
         return get().positiveWords.contains(key);
      }
   }

}
