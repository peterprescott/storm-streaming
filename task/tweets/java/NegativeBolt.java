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
 * Simple Bolt that check words of incoming sentence and mark sentence with a negative score.
 * 
 * @author Adrianos Dadis
 * 
 */
public class NegativeBolt extends BaseBasicBolt {
   private static final long serialVersionUID = 1L;
   private static final Logger LOG = LoggerFactory.getLogger(NegativeBolt.class);

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
         int negativeWordsSize = 0;
         for (String word : words) {
            if (NegativeWords.get().contains(word)) {
               negativeWordsSize++;
            }
         }

         node.put(Cons.NUM_NEGATIVE, (double) negativeWordsSize / wordsSize);

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

   private static class NegativeWords {
      private Set<String> negativeWords;
      private static NegativeWords _singleton;

      private static Set<String> NEGATIVE_WORDS = new HashSet<String>(Arrays.asList(new String[] {
	"abysmal", "adverse", "alarming", "angry", "annoy", "anxious", "apathy", "appalling", "atrocious", "awful", "bad", 
	"banal", "barbed", "belligerent", "bemoan", "beneath", "boring", "broken", "callous", "can't", "clumsy", "coarse", 
	"cold", "cold-hearted", "collapse", "confused", "contradictory", "contrary", "corrosive", "corrupt", "crazy", "creepy",
	"criminal", "cruel", "cry", "cutting", "dead", "decaying", "damage", "damaging", "dastardly", "deplorable", "depressed", 
	"deprived", "deformed", "deny", "despicable", "detrimental", "dirty", "disease", "disgusting", "disheveled", "dishonest", 
	"dishonorable", "dismal", "distress", "don't", "dreadful", "dreary", "enraged", "eroding", "evil", "fail", "faulty", 
	"fear", "feeble", "fight", "filthy", "foul", "frighten", "frightful", "gawky", "ghastly", "grave", "greed", "grim", 
	"grimace", "gross", "grotesque", "gruesome", "guilty", "haggard", "hard", "hard-hearted", "harmful", "hate", "hideous", 
	"homely", "horrendous", "horrible", "hostile", "hurt", "hurtful", "icky", "ignore", "ignorant", "ill", "immature", 
	"imperfect", "impossible", "inane", "inelegant", "infernal", "injure", "injurious", "insane", "insidious", "insipid", 
	"jealous", "junky", "lose", "lousy", "lumpy", "malicious", "mean", "menacing", "messy", "misshapen", "missing", 
	"misunderstood", "moan", "moldy", "monstrous", "naive", "nasty", "naughty", "negate", "negative", "never", "no", 
	"nobody", "nondescript", "nonsense", "not", "noxious", "objectionable", "odious", "offensive", "old", "oppressive", 
	"pain", "perturb", "pessimistic", "petty", "plain", "poisonous", "poor", "prejudice", "questionable", "quirky", "quit", 
	"reject", "renege", "repellant", "reptilian", "repulsive", "repugnant", "revenge", "revolting", "rocky", "rotten", "rude", 
	"ruthless", "sad", "savage", "scare", "scary", "scream", "severe", "shoddy", "shocking", "sick", "sickening", "sinister", 
	"slimy", "smelly", "sobbing", "sorry", "spiteful", "sticky", "stinky", "stormy", "stressful", "stuck", "stupid", 
	"substandard", "suspect", "suspicious", "tense", "terrible", "terrifying", "threatening", "ugly", "undermine", "unfair", 
	"unfavorable", "unhappy", "unhealthy", "unjust", "unlucky", "unpleasant", "upset", "unsatisfactory", "unsightly", 
	"untoward", "unwanted", "unwelcome", "unwholesome", "unwieldy", "unwise", "upset", "vice", "vicious", "vile", "villainous", 
	"vindictive", "wary", "weary", "wicked", "woeful", "worthless", "wound", "yell", "yucky", "zero",
	}));

      static NegativeWords get() {
         if (_singleton == null) {
            synchronized (NegativeWords.class) {
               if (_singleton == null) {
                  _singleton = new NegativeWords();
               }
            }
         }

         return _singleton;
      }

      boolean contains(String key) {
         return get().negativeWords.contains(key);
      }
   }

}
