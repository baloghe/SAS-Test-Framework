# SAS-Test-Framework
<h3>Database development testing framework in SAS</h3>
<p>Inspired by the JUnit framework, that is: provide a framework for process Unit tests SAS datawarehouse development in the usual <i>Expected vs Actual value</i> wherever possible. Speaking about a database, test results (both OK and NOK (=Not OK)) are saved in a log table. Speaking about SAS, the implementation is heavily built on the macro facility.</p>
<p>Features (occasionally just planned ones):</p>
<ul>
	<li>program <b>static_create:</b> log table and metatables definition (i.e. what to test against which expectations) </li>
	<li>macro <b>test_single_source:</b> 1:1 table comparison with renamed fields and custom filtering on both sides (e.g. for testing Stage loaders)</li>
	<li>macro <b>test_fillup_percent:</b> test ratio of non-missing values in a field (optional: custom filtering) </li>
	<li>macro <b>test_contains_values:</b> test if a field contains the given list of values (at least once; optional: custom filtering)</li>
	<li>demo</li>
</ul>
