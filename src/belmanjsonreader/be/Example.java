package belmanjsonreader.be;


/**
 *
 * @author Abdil-K
 */
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import com.fasterxml.jackson.annotation.JsonAnyGetter;
import com.fasterxml.jackson.annotation.JsonAnySetter;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;

@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder({
"AvailableWorkers",
"ProductionOrders"
})
public class Example {

@JsonProperty("AvailableWorkers")
private List<AvailableWorker> availableWorkers = null;
@JsonProperty("ProductionOrders")
private List<ProductionOrder> productionOrders = null;
@JsonIgnore
private Map<String, Object> additionalProperties = new HashMap<String, Object>();

@JsonProperty("AvailableWorkers")
public List<AvailableWorker> getAvailableWorkers() {
return availableWorkers;
}

@JsonProperty("AvailableWorkers")
public void setAvailableWorkers(List<AvailableWorker> availableWorkers) {
this.availableWorkers = availableWorkers;
}

@JsonProperty("ProductionOrders")
public List<ProductionOrder> getProductionOrders() {
return productionOrders;
}

@JsonProperty("ProductionOrders")
public void setProductionOrders(List<ProductionOrder> productionOrders) {
this.productionOrders = productionOrders;
}

@JsonAnyGetter
public Map<String, Object> getAdditionalProperties() {
return this.additionalProperties;
}

@JsonAnySetter
public void setAdditionalProperty(String name, Object value) {
this.additionalProperties.put(name, value);
}

}