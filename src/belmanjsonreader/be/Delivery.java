/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package belmanjsonreader.be;

/**
 *
 * @author Abdil-K
 */
import java.util.HashMap;
import java.util.Map;
import com.fasterxml.jackson.annotation.JsonAnyGetter;
import com.fasterxml.jackson.annotation.JsonAnySetter;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;

@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder({
"__type",
"DeliveryTime"
})
public class Delivery {

@JsonProperty("__type")
private String type;
@JsonProperty("DeliveryTime")
private String deliveryTime;
@JsonIgnore
private Map<String, Object> additionalProperties = new HashMap<String, Object>();

@JsonProperty("__type")
public String getType() {
return type;
}

@JsonProperty("__type")
public void setType(String type) {
this.type = type;
}

@JsonProperty("DeliveryTime")
public String getDeliveryTime() {
return deliveryTime;
}

@JsonProperty("DeliveryTime")
public void setDeliveryTime(String deliveryTime) {
this.deliveryTime = deliveryTime;
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