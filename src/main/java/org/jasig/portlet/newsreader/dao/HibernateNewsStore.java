/**
 * Licensed to Apereo under one or more contributor license
 * agreements. See the NOTICE file distributed with this work
 * for additional information regarding copyright ownership.
 * Apereo licenses this file to you under the Apache License,
 * Version 2.0 (the "License"); you may not use this file
 * except in compliance with the License.  You may obtain a
 * copy of the License at the following location:
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.jasig.portlet.newsreader.dao;

import java.util.List;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.hibernate.Query;
import org.hibernate.SessionFactory;
import org.jasig.portlet.newsreader.NewsConfiguration;
import org.jasig.portlet.newsreader.NewsDefinition;
import org.jasig.portlet.newsreader.NewsSet;
import org.jasig.portlet.newsreader.PredefinedNewsConfiguration;
import org.jasig.portlet.newsreader.PredefinedNewsDefinition;
import org.jasig.portlet.newsreader.UserDefinedNewsConfiguration;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;


/**
 * HibernateNewsStore provides a hibernate implementation of the NewsStore.
 *
 * @author Anthony Colebourne
 * @author Jen Bourey
 */
@Service
@Transactional
public class HibernateNewsStore implements NewsStore {

    private static Log log = LogFactory.getLog(HibernateNewsStore.class);

    @Override
    public void storeNewsDefinition(NewsDefinition listing) {
       	sessionFactory.getCurrentSession().saveOrUpdate(listing);
       	sessionFactory.getCurrentSession().flush();
    }

    @Override
    public void storeNewsConfiguration(NewsConfiguration configuration) {
       	sessionFactory.getCurrentSession().saveOrUpdate(configuration);
       	sessionFactory.getCurrentSession().flush();
    }

    @Override
	public List<NewsConfiguration> getNewsConfigurations(String subscribeId) {
    	log.debug("fetching news configurations for " + subscribeId);
    	
    	Query query = sessionFactory.getCurrentSession().createQuery(
                    "from NewsConfiguration config where "
                            + "subscribeId = :subscribeId and displayed = true ");
    	query.setParameter("subscribeId", subscribeId);
    	return (List<NewsConfiguration>) query.list();
    }

    @Override
    public List<UserDefinedNewsConfiguration> getUserDefinedNewsConfigurations(Long setId, boolean visibleOnly) {
    	String hql = "from NewsConfiguration config where "
                    + "config.newsSet.id = :id and "
                    + "config.class = UserDefinedNewsConfiguration "
                    + "order by newsDefinition.name";
    	if (visibleOnly) {
    		hql = hql.concat(" and visibleOnly = true");
    	}

    	Query query = sessionFactory.getCurrentSession().createQuery(hql);
    	query.setParameter("id", setId);
    	
    	return (List<UserDefinedNewsConfiguration>) query.list();
    }

    @Override
    public List<PredefinedNewsConfiguration> getPredefinedNewsConfigurations(
            Long setId, boolean visibleOnly) {

    	String hql = "from NewsConfiguration config "
                    + "where config.newsSet.id = :id and "
                    + "config.class = PredefinedNewsConfiguration "
                    + "order by newsDefinition.name";
    	if (visibleOnly) {
    		hql = hql.concat(" and visibleOnly = true");
    	}

    	Query query = sessionFactory.getCurrentSession().createQuery(hql);
    	query.setParameter("id", setId);
    	
    	return (List<PredefinedNewsConfiguration>) query.list(); 
    }

    @Override
    public List<PredefinedNewsConfiguration> getPredefinedNewsConfigurations() {
            String query = "from NewsDefinition def "
                    + "where def.class = PredefinedNewsDefinition "
                    + "order by def.name";
            return (List<PredefinedNewsConfiguration>) sessionFactory.getCurrentSession()
                    .find(query);
    }

    @Override
    public List<PredefinedNewsDefinition> getHiddenPredefinedNewsDefinitions(Long setId, Set<String> roles) {
    	String query = "from PredefinedNewsDefinition def "
                    + "where :setId not in (select config.newsSet.id "
                    + "from def.userConfigurations config) ";
    	for (int i = 0; i < roles.size(); i++) {
    		query = query.concat(
                        "and :role" + i + " not in elements(def.defaultRoles) ");
    	}

    	Query q = sessionFactory.getCurrentSession().createQuery(query);
    	q.setLong("setId", setId);
    	int count = 0;
    	for (String role : roles) {
    		q.setString("role" + count, role);
    		count++;
    	}
    	return (List<PredefinedNewsDefinition>) q.list();
    }

    @Override
    public void initNews(NewsSet set, Set<String> roles) {
    	//if the user doesn't have any roles, we don't have any
    	//chance of getting predefined newss, so just go ahead
    	//and return
    	if (roles.isEmpty()) {
    		return;
    	}

    	String query = "from PredefinedNewsDefinition def "
                    + "left join fetch def.defaultRoles role where "
                    + ":setId not in (select config.newsSet.id "
                    + "from def.userConfigurations config)";
    	if (roles.size() > 0) {
    		query = query.concat("and role in (:roles)");
    	}
    	Query q = sessionFactory.getCurrentSession().createQuery(query);
    	q.setLong("setId", set.getId());
    	if (roles.size() > 0) {
    		q.setParameterList("roles", roles);
    	}
    	List<PredefinedNewsDefinition> defs = q.list();

    	for (PredefinedNewsDefinition def : defs) {
    		PredefinedNewsConfiguration config = new PredefinedNewsConfiguration();
    		config.setNewsDefinition(def);
    		set.addNewsConfiguration(config);
    	}
    }

    @Override
    public PredefinedNewsDefinition getPredefinedNewsDefinition(Long id) {
            String query = "from PredefinedNewsDefinition def "
                    + "left join fetch def.defaultRoles role where "
                    + "def.id = :id";
            Query q = sessionFactory.getCurrentSession().createQuery(query);
            q.setLong("id", id);
            return (PredefinedNewsDefinition) q.uniqueResult();
    }

    @Override
    public PredefinedNewsDefinition getPredefinedNewsDefinitionByName(String name) {
    	String query = "from PredefinedNewsDefinition def "
                    + "left join fetch def.defaultRoles role where "
                    + "def.name = :name";
    	Query q = sessionFactory.getCurrentSession().createQuery(query);
    	q.setString("name", name);
    	return (PredefinedNewsDefinition) q.uniqueResult();
    }

    @Override
    public NewsDefinition getNewsDefinition(Long id) {
    	return (NewsDefinition) sessionFactory.getCurrentSession().get(NewsDefinition.class, id);
    }

    @Override
    public NewsConfiguration getNewsConfiguration(Long id) {
    	return (NewsConfiguration) sessionFactory.getCurrentSession().load(NewsConfiguration.class, id);
    }

    @Override
    public void deleteNewsConfiguration(NewsConfiguration configuration) {
    	sessionFactory.getCurrentSession().delete(configuration);
    	sessionFactory.getCurrentSession().flush();
    }

    @Override
    public void deleteNewsDefinition(PredefinedNewsDefinition definition) {
    	String hql = "from NewsConfiguration config "
                + "where config.newsDefinition.id = :id and "
                + "config.class = PredefinedNewsConfiguration";

    	Query query = sessionFactory.getCurrentSession().createQuery(hql);
    	query.setParameter("id", definition.getId());
    	
    	List<PredefinedNewsConfiguration> configs = query.list();
    	
    	sessionFactory.getCurrentSession().delete(configs);
    	sessionFactory.getCurrentSession().delete(definition);
    	sessionFactory.getCurrentSession().flush();
    }

    @Override
    public List<String> getUserRoles() {
    	String query = "select distinct elements(def.defaultRoles) " +
                    "from PredefinedNewsDefinition def ";

    	return (List<String>) sessionFactory.getCurrentSession().find(query);
    }

    @Override
	public NewsSet getNewsSet(Long id) {
		return (NewsSet) sessionFactory.getCurrentSession().get(NewsSet.class, id);
	}

    @Override
	public List<NewsSet> getNewsSetsForUser(String userId) {
		log.debug("fetching news sets for " + userId);
		
		Query query = sessionFactory.getCurrentSession().createQuery(
                    "from NewsSet newsSet where "
                            + "newsSet.userId = :id "
                            + "order by newsSet.name");
		query.setParameter("id", userId);
		
		return (List<NewsSet>) query.list();
	}

    @Override
	public void storeNewsSet(NewsSet set) {
		sessionFactory.getCurrentSession().saveOrUpdate(set);
		sessionFactory.getCurrentSession().flush();
	}

    @Override
	public NewsSet getNewsSet(String userId, String setName) {
		log.debug("fetching news sets for " + userId);
		String query = "from NewsSet newsSet where :userId = newsSet.userId and " +
            		":setName = newsSet.name order by newsSet.name";

		Query q = sessionFactory.getCurrentSession().createQuery(query);
		q.setString("userId", userId);
		q.setString("setName", setName);
		log.debug("Size of result set" + q.list().size());
		return (NewsSet) q.uniqueResult();
	}

	@Autowired
	SessionFactory sessionFactory;	
}
