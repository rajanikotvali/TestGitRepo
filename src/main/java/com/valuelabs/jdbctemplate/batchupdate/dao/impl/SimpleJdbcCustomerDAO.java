/**
 * 
 */
package com.valuelabs.jdbctemplate.batchupdate.dao.impl;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.springframework.jdbc.core.namedparam.BeanPropertySqlParameterSource;
import org.springframework.jdbc.core.namedparam.SqlParameterSource;
import org.springframework.jdbc.core.namedparam.SqlParameterSourceUtils;
import org.springframework.jdbc.core.simple.ParameterizedBeanPropertyRowMapper;
import org.springframework.jdbc.core.simple.SimpleJdbcDaoSupport;

import com.valuelabs.jdbctemplate.batchupdate.model.Customer;
import com.valuelabs.jdbctemplate.batchupdate.model.CustomerParameterizedRowMapper;
import com.valuelabs.jdbctemplate.batchupdate.dao.CustomerDAO;

/**
 * @author rkotvali
 *
 */
public class SimpleJdbcCustomerDAO extends SimpleJdbcDaoSupport implements
CustomerDAO {

	/* (non-Javadoc)
	 * @see com.valuelabs.jdbctemplate.batchupdate.dao.CustomerDAO#insertBatchSQL(java.lang.String)
	 * insert batch example with SQL
	 */
	@Override
	public void insertBatchSQL(String sql) {
		getJdbcTemplate().batchUpdate(new String[]{sql});

	}

	/* (non-Javadoc)
	 * @see com.valuelabs.jdbctemplate.batchupdate.dao.CustomerDAO#findCustomerNameById(int)
	 */
	@Override
	public String findCustomerNameById(int custId) {
		String sql = "SELECT NAME FROM CUSTOMER WHERE CUST_ID = ?";

		String name = getSimpleJdbcTemplate().queryForObject(sql, String.class, custId);

		return name;
	}

	/* (non-Javadoc)
	 * @see com.valuelabs.jdbctemplate.batchupdate.dao.CustomerDAO#findTotalCustomer()
	 */
	@Override
	public int findTotalCustomer() {
		String sql = "SELECT COUNT(*) FROM CUSTOMER";

		int total = getSimpleJdbcTemplate().queryForInt(sql);

		return total;
	}

	/* (non-Javadoc)
	 * @see com.valuelabs.jdbctemplate.batchupdate.dao.CustomerDAO#insert(com.valuelabs.jdbctemplate.batchupdate.dao.Customer)
	 * insert example
	 */
	public void insert(Customer customer) {
		String sql = "INSERT INTO CUSTOMER " +
				"(CUST_ID, NAME, AGE) VALUES (?, ?, ?)";

		getSimpleJdbcTemplate().update(sql, customer.getCustId(), customer.getName(),customer.getAge());

	}

	/* (non-Javadoc)
	 * @see com.valuelabs.jdbctemplate.batchupdate.dao.CustomerDAO#insertNamedParameter(com.valuelabs.jdbctemplate.batchupdate.dao.Customer)
	 * insert with named parameter
	 */
	public void insertNamedParameter(Customer customer) {
		String sql = "INSERT INTO CUSTOMER " + "(CUST_ID, NAME, AGE) VALUES (:custId, :name, :age)";

		Map<String, Object> parameters = new HashMap<String, Object>();
		parameters.put("custId", customer.getCustId());
		parameters.put("name", customer.getName());
		parameters.put("age", customer.getAge());

		getSimpleJdbcTemplate().update(sql, parameters);

	}

	/* (non-Javadoc)
	 * @see com.valuelabs.jdbctemplate.batchupdate.dao.CustomerDAO#insertBatch(com.valuelabs.jdbctemplate.batchupdate.dao.List)
	 * insert batch example
	 */
	public void insertBatch(final List<Customer> customer) {
		String sql = "INSERT INTO CUSTOMER " +
				"(CUST_ID, NAME, AGE) VALUES (?, ?, ?)";

		List<Object[]> parameters = new ArrayList<Object[]>();
		for (Customer cust : customer) {
			parameters.add(new Object[] {cust.getCustId(), cust.getName(), cust.getAge()});
		}
		getSimpleJdbcTemplate().batchUpdate(sql, parameters);

	}

	/* (non-Javadoc)
	 * @see com.valuelabs.jdbctemplate.batchupdate.dao.CustomerDAO#insertBatchNamedParameter(com.valuelabs.jdbctemplate.batchupdate.dao.List)
	 * insert batch with named parameter
	 */
	public void insertBatchNamedParameter(final List<Customer> customer) {
		String sql = "INSERT INTO CUSTOMER " +
				"(CUST_ID, NAME, AGE) VALUES (:custId, :name, :age)";

		List<SqlParameterSource> parameters = new ArrayList<SqlParameterSource>();
		for (Customer cust : customer) {

			parameters.add(new BeanPropertySqlParameterSource(cust));

		}
		getSimpleJdbcTemplate().batchUpdate(sql,
				parameters.toArray(new SqlParameterSource[0]));

	}

	/* (non-Javadoc)
	 * @see com.valuelabs.jdbctemplate.batchupdate.dao.CustomerDAO#insertBatchNamedParameter2(com.valuelabs.jdbctemplate.batchupdate.dao.List)
	 * insert batch with named parameter
	 */
	public void insertBatchNamedParameter2(final List<Customer> customer) {
		SqlParameterSource[] params = SqlParameterSourceUtils.createBatch(customer.toArray());
		getSimpleJdbcTemplate().batchUpdate(
				"INSERT INTO CUSTOMER (CUST_ID, NAME, AGE) VALUES (:custId, :name, :age)",
				params);

	}

	/* (non-Javadoc)
	 * @see com.valuelabs.jdbctemplate.batchupdate.dao.CustomerDAO#findByCustomerId(int)
	 * query single row with ParameterizedRowMapper
	 */
	public Customer findByCustomerId(int custId) {
		String sql = "SELECT * FROM CUSTOMER WHERE CUST_ID = ?";

		Customer customer = getSimpleJdbcTemplate().queryForObject(
				sql,  new CustomerParameterizedRowMapper(), custId);

		return customer;
	}

	/* (non-Javadoc)
	 * @see com.valuelabs.jdbctemplate.batchupdate.dao.CustomerDAO#findByCustomerId2(int)
	 * query single row with ParameterizedBeanPropertyRowMapper (Customer.class)
	 */
	public Customer findByCustomerId2(int custId) {
		String sql = "SELECT * FROM CUSTOMER WHERE CUST_ID = ?";

		Customer customer = getSimpleJdbcTemplate().queryForObject(
				sql,ParameterizedBeanPropertyRowMapper.newInstance(Customer.class), custId);

		return customer;
	}

	/* (non-Javadoc)
	 * @see com.valuelabs.jdbctemplate.batchupdate.dao.CustomerDAO#findAll()
	 * query mutiple rows with ParameterizedBeanPropertyRowMapper (Customer.class)
	 */
	public List<Customer> findAll() {
		String sql = "SELECT * FROM CUSTOMER";

		List<Customer> customers = 
				getSimpleJdbcTemplate().query(sql, ParameterizedBeanPropertyRowMapper.newInstance(Customer.class));

		return customers;
	}

	/* (non-Javadoc)
	 * @see com.valuelabs.jdbctemplate.batchupdate.dao.CustomerDAO#findAll2()
	 * query mutiple rows with ParameterizedBeanPropertyRowMapper (Customer.class)
	 */
	public List<Customer> findAll2() {
		String sql = "SELECT * FROM CUSTOMER";

		List<Customer> customers = 
				getSimpleJdbcTemplate().query(sql, ParameterizedBeanPropertyRowMapper.newInstance(Customer.class));

		return customers;
	}
	
	
	
	

}
