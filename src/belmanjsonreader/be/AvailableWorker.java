/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package belman.be;

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
"Initials",
"Name",
"SalaryNumber"
})
public class AvailableWorker {

@JsonProperty("__type")
private String type;
@JsonProperty("Initials")
private String initials;
@JsonProperty("Name")
private String name;
@JsonProperty("SalaryNumber")
private Integer salaryNumber;
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

@JsonProperty("Initials")
public String getInitials() {
return initials;
}

@JsonProperty("Initials")
public void setInitials(String initials) {
this.initials = initials;
}

@JsonProperty("Name")
public String getName() {
return name;
}

@JsonProperty("Name")
public void setName(String name) {
this.name = name;
}

@JsonProperty("SalaryNumber")
public Integer getSalaryNumber() {
return salaryNumber;
}

@JsonProperty("SalaryNumber")
public void setSalaryNumber(Integer salaryNumber) {
this.salaryNumber = salaryNumber;
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
