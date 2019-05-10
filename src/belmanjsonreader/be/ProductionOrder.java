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
"__type",
"Customer",
"Delivery",
"DepartmentTasks",
"Order"
})
public class ProductionOrder {

@JsonProperty("__type")
private String type;
@JsonProperty("Customer")
private Customer customer;
@JsonProperty("Delivery")
private Delivery delivery;
@JsonProperty("DepartmentTasks")
private List<DepartmentTask> departmentTasks = null;
@JsonProperty("Order")
private Order order;
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

@JsonProperty("Customer")
public Customer getCustomer() {
return customer;
}

@JsonProperty("Customer")
public void setCustomer(Customer customer) {
this.customer = customer;
}

@JsonProperty("Delivery")
public Delivery getDelivery() {
return delivery;
}

@JsonProperty("Delivery")
public void setDelivery(Delivery delivery) {
this.delivery = delivery;
}

@JsonProperty("DepartmentTasks")
public List<DepartmentTask> getDepartmentTasks() {
return departmentTasks;
}

@JsonProperty("DepartmentTasks")
public void setDepartmentTasks(List<DepartmentTask> departmentTasks) {
this.departmentTasks = departmentTasks;
}

@JsonProperty("Order")
public Order getOrder() {
return order;
}

@JsonProperty("Order")
public void setOrder(Order order) {
this.order = order;
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
