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
"Department",
"EndDate",
"FinishedOrder",
"StartDate"
})
public class DepartmentTask {

@JsonProperty("__type")
private String type;
@JsonProperty("Department")
private Department department;
@JsonProperty("EndDate")
private String endDate;
@JsonProperty("FinishedOrder")
private Boolean finishedOrder;
@JsonProperty("StartDate")
private String startDate;
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

@JsonProperty("Department")
public Department getDepartment() {
return department;
}

@JsonProperty("Department")
public void setDepartment(Department department) {
this.department = department;
}

@JsonProperty("EndDate")
public String getEndDate() {
return endDate;
}

@JsonProperty("EndDate")
public void setEndDate(String endDate) {
this.endDate = endDate;
}

@JsonProperty("FinishedOrder")
public Boolean getFinishedOrder() {
return finishedOrder;
}

@JsonProperty("FinishedOrder")
public void setFinishedOrder(Boolean finishedOrder) {
this.finishedOrder = finishedOrder;
}

@JsonProperty("StartDate")
public String getStartDate() {
return startDate;
}

@JsonProperty("StartDate")
public void setStartDate(String startDate) {
this.startDate = startDate;
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