package com.abc;

import java.util.HashSet;
import java.util.Set;

import org.bson.types.ObjectId;
import org.mongodb.morphia.annotations.Entity;
import org.mongodb.morphia.annotations.Field;
import org.mongodb.morphia.annotations.Id;
import org.mongodb.morphia.annotations.Index;
import org.mongodb.morphia.annotations.IndexOptions;
import org.mongodb.morphia.annotations.Indexes;
import org.mongodb.morphia.annotations.Property;
import org.mongodb.morphia.annotations.Reference;
import org.mongodb.morphia.annotations.Version;

@Entity("person")
@Indexes(
	    @Index(options = @IndexOptions(name = "person_index"), 
	    		fields = {@Field("lastName"), @Field("firstName")})
)
public class Person {
	@Id
	private ObjectId id;
	@Property
	private String firstName;
	@Property
	private String lastName;
	@Property
	private String sex;
	@Reference
	private Set<Address> addresses = null;
	@Version 
	private long v;

	public ObjectId getId() {
		return id;
	}

	public void setId(ObjectId id) {
		this.id = id;
	}

	public String getFirstName() {
		return firstName;
	}

	public void setFirstName(String firstName) {
		this.firstName = firstName;
	}

	public String getLastName() {
		return lastName;
	}

	public void setLastName(String lastName) {
		this.lastName = lastName;
	}
	
	public String getSex() {
		return sex;
	}

	public void setSex(String sex) {
		this.sex = sex;
	}

	public Set<Address> getAddress() {
		if (addresses == null)
			addresses = new HashSet<Address>();
		return addresses;
	}

	public void setAddresses(Set<Address> addresses) {
		this.addresses = addresses;
	}
	
	@Override
	public String toString(){
		return "{" + id + ", " + firstName + ", " + lastName  + ", " 
				+ sex + ", "
				+ addresses + "}";
	}
}