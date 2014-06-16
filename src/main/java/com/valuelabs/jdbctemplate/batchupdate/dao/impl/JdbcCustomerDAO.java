/**
 * 
 */
package com.valuelabs.jdbctemplate.batchupdate.dao.impl;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.springframework.jdbc.core.BatchPreparedStatementSetter;
import org.springframework.jdbc.core.BeanPropertyRowMapper;
import org.springframework.jdbc.core.support.JdbcDaoSupport;

import com.valuelabs.jdbctemplate.batchupdate.dao.CustomerDAO;
import com.valuelabs.jdbctemplate.batchupdate.model.Customer;
import com.valuelabs.jdbctemplate.batchupdate.model.CustomerRowMapper;

/**
 * @author rkotvali
 *
 */
public class JdbcCustomerDAO extends JdbcDaoSupport implements CustomerDAO {

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

		String name = (String)getJdbcTemplate().queryForObject(sql, new Object[] { custId }, String.class);

		return name;
	}

	/* (non-Javadoc)
	 * @see com.valuelabs.jdbctemplate.batchupdate.dao.CustomerDAO#findTotalCustomer()
	 */
	@Override
	public int findTotalCustomer() {
		String sql = "SELECT COUNT(*) FROM CUSTOMER";

		int total = getJdbcTemplate().queryForInt(sql);

		return total;
	}

	/* (non-Javadoc)
	 * @see com.valuelabs.jdbctemplate.batchupdate.dao.CustomerDAO#insert(com.valuelabs.jdbctemplate.batchupdate.dao.Customer)
	 * insert example
	 */
	public void insert(Customer customer) {
		String sql = "INSERT INTO CUSTOMER " + "(CUST_ID, NAME, AGE) VALUES (?, ?, ?)";

		getJdbcTemplate().update(sql, new Object[] { customer.getCustId(),
				customer.getName(),customer.getAge()  
		});

	}

	/* (non-Javadoc)
	 * @see com.valuelabs.jdbctemplate.batchupdate.dao.CustomerDAO#insertNamedParameter(com.valuelabs.jdbctemplate.batchupdate.dao.Customer)
	 * insert with named parameter
	 */
	public void insertNamedParameter(Customer customer) {
		// not supported

	}

	/* (non-Javadoc)
	 * @see com.valuelabs.jdbctemplate.batchupdate.dao.CustomerDAO#insertBatch(com.valuelabs.jdbctemplate.batchupdate.dao.List)
	 * insert batch example
	 */
	public void insertBatch(final List<Customer> customers) {
		String sql = "INSERT INTO CUSTOMER " +
				"(CUST_ID, NAME, AGE) VALUES (?, ?, ?)";

		getJdbcTemplate().batchUpdate(sql, new BatchPreparedStatementSetter() {

			@Override
			public void setValues(PreparedStatement ps, int i) throws SQLException {
				Customer customer = customers.get(i);
				ps.setLong(1, customer.getCustId());
				ps.setString(2, customer.getName());
				ps.setInt(3, customer.getAge() );
			}

			@Override
			public int getBatchSize() {
				return customers.size();
			}
		});
	}

	/* (non-Javadoc)
	 * @see com.valuelabs.jdbctemplate.batchupdate.dao.CustomerDAO#insertBatchNamedParameter(com.valuelabs.jdbctemplate.batchupdate.dao.List)
	 * insert batch with named parameter
	 */
	public void insertBatchNamedParameter(final List<Customer> customer) {
		// not supported

	}

	/* (non-Javadoc)
	 * @see com.valuelabs.jdbctemplate.batchupdate.dao.CustomerDAO#insertBatchNamedParameter2(com.valuelabs.jdbctemplate.batchupdate.dao.List)
	 * insert batch with named parameter
	 */
	public void insertBatchNamedParameter2(final List<Customer> customer) {
		// not supported

	}

	/* (non-Javadoc)
	 * @see com.valuelabs.jdbctemplate.batchupdate.dao.CustomerDAO#findByCustomerId(int)
	 * query single row with RowMapper
	 */
	public Customer findByCustomerId(int custId) {
		String sql = "SELECT * FROM CUSTOMER WHERE CUST_ID = ?";

		Customer customer = (Customer)getJdbcTemplate().queryForObject(sql, new Object[] { custId }, new CustomerRowMapper());

		return customer;
	}

	/* (non-Javadoc)
	 * @see com.valuelabs.jdbctemplate.batchupdate.dao.CustomerDAO#findByCustomerId2(int)
	 * query single row with BeanPropertyRowMapper (Customer.class)
	 */
	public Customer findByCustomerId2(int custId) {
		String sql = "SELECT * FROM CUSTOMER WHERE CUST_ID = ?";

		Customer customer = (Customer)getJdbcTemplate().queryForObject(sql, new Object[] { custId }, new BeanPropertyRowMapper(Customer.class));

		return customer;
	}

	/* (non-Javadoc)
	 * @see com.valuelabs.jdbctemplate.batchupdate.dao.CustomerDAO#findAll()
	 * query mutiple rows with manual mapping
	 */
	@Override
	public List<Customer> findAll() {
		String sql = "SELECT * FROM CUSTOMER";

		List<Customer> customers = new ArrayList<Customer>();

		List<Map<String, Object>> rows = getJdbcTemplate().queryForList(sql);
		for (Map row : rows) {
			Customer customer = new Customer();
			customer.setCustId((Long)(row.get("CUST_ID")));
			customer.setName((String)row.get("NAME"));
			customer.setAge((Integer)row.get("AGE"));
			customers.add(customer);
		}

		return customers;
	}

	/* (non-Javadoc)
	 * @see com.valuelabs.jdbctemplate.batchupdate.dao.CustomerDAO#findAll2()
	 * query mutiple rows with BeanPropertyRowMapper (Customer.class)
	 */
	@Override
	public List<Customer> findAll2() {
		String sql = "SELECT * FROM CUSTOMER";

		List<Customer> customers  = getJdbcTemplate().query(sql, new BeanPropertyRowMapper(Customer.class));

		return customers;
	}

}
