% Author: Mohammad Dehghani Ashkezari <mdehghan@uw.edu>
% 
% Date: 2019-12-17
% 
% Function: Constructs the fundamentals of CMAP API calls using MATLAB.
%
%
% CMAP class handles RESTful requests to the Simons CMAP API. 
% To retrieve data form the Simons CMAP database you need to get an API key 
% from https://simonscmap.com and store it on your machine permanently using 
% the command below:
%
% CMAP.set_api_key('your_api_key');
%
% This package is adopted from 'pycmap' which is the python client of Simons 
% CMAP ecosystem (https://github.com/simonscmap/pycmap).  


classdef CMAP
           
    properties
        apiKey 
    end

    methods
        function obj = CMAP(token)
            % CMAP constructor method.
            % :param str token: access token to make client requests.
            %
            % The code below instantiates the CMAP class and registers the
            % API key:
            %
            % cmap = CMAP('your api key');
            %
            % To register the API key, you may use the 'set_api_key'
            % method, alternatively:
            % 
            % CMAP.set_api_key('your api key');
            %
            % See also set_api_key
            

            if nargin < 1
                token = CMAP.get_api_key();                
            end    
            obj.apiKey = token;
        end
        
        function obj = set.apiKey(obj, value)
            obj.apiKey = value;
            CMAP.set_api_key(value); 
        end      
        
    end
    
    
    methods (Static)       
        function apiKey = get_api_key()
            % Returns CMAP API Key previously stored in a system variable (see set_api_key(api_key) function).
            %
            % See also set_api_key
            
            apiKey = getenv('CMAP_API_KEY');
            if isempty(apiKey)
                load('api_key.mat', 'api_key');
                apiKey = api_key;
            end    
            if isempty(apiKey)
                error('\n\n%s \n%s \n%s \n%s\n',... 
                      'CMAP API Key not found.',... 
                      'You may obtain an API Key from https://simonscmap.com.',... 
                      'Record your API key on your machine permanently using the following command:',...
                      'CMAP.set_api_key(''<Your API Key>'');'...
                      )
            end   
        end
                
        
        function set_api_key(api_key)
            % Stores the API Key in a system variable.
            % The API key will be stored permanently on your machine,
            % Therefore, you only need to call this method once.
            %
            % Alternatively, the API key may be registered on your machine
            % using the CMAP constructor:
            % CMAP('your api key');

            % turned out on macOS Catalina setenv function doesn't store
            % the system variable permanently. So we also keep the key on a
            % local file.
            save('api_key.mat', 'api_key');            
            
            CMAP.apiKey = api_key;
            setenv('CMAP_API_KEY', api_key);            
        end    
        

        function queryString = encode_payload(payload)
            % Constructs the encoded query string to be added to the base API URL
            % (domain+route). payload holds the query parameters and their values.
            
            fn = fieldnames(payload);
            queryString = '';
            for k=1:numel(fn)
                queryString = strcat(queryString, fn{k}, '=', urlencode(payload.(fn{k})));
                if k < numel(fn)
                    queryString = strcat(queryString, '&');
                end    
            end
        end

        function tbl = atomic_request(route, payload)
            % Submits a single GET request. 
            % Returns the body in form of a MATALAB table if 200 status.
            
            import matlab.net.*
            import matlab.net.http.*

            baseURL = 'https://simonscmap.com';
            queryString = CMAP.encode_payload(payload);
            uri = strcat(baseURL, route, queryString);          
            r = RequestMessage('GET');
            prefixeKey = char(strcat('Api-Key', {' '}, CMAP.get_api_key()));
            field = matlab.net.http.field.GenericField('Authorization', prefixeKey);
            r = addFields(r, field);            
            options = matlab.net.http.HTTPOptions('ConnectTimeout', 2000);            
            [resp, ~, ~] = send(r, uri, options);
            status = getReasonPhrase(resp.StatusCode);

            tbl = CMAP.resp_to_table(resp.Body.Data);    
            
            if ~strcmp(char(status), 'OK')
                 disp(strcat('Status:', {' '}, status))
                 disp(strcat('Status Code:', {' '}, num2str(resp.StatusCode)))
                 disp(strcat('Message:', {' '}, char(resp.Body.Data)'))
             end    
        end

        
        function tbl = resp_to_table(respData)
            % Saves the response data in a csv file. 
            % The csv file is then deleted after is read into a table variable.
            
            % TODO: see if it's possible to directly convert the response to a table?
            % TODO: resp.Body.Data >> table variable
            fname = 'resp.csv';
            fid = fopen(fname, 'wt');
            fwrite(fid, respData);
            fclose(fid);
            tbl = readtable(fname);
            delete(fname);                             
        end
        
        
        
        function tbl = query(queryString)
            % Takes a custom query and returns the results in form of a table.
            
            payload = struct('query', queryString);
            tbl = CMAP.atomic_request('/api/data/query?', payload);
        end
        
        
        function tbl = stored_proc(args)
            % Executes a strored-procedure and returns the results in form of a table.
            
            payload = struct('tableName', args(1), 'fields', args(2), 'dt1', args(3), 'dt2', args(4), 'lat1', args(5), 'lat2', args(6), 'lon1', args(7), 'lon2', args(8), 'depth1', args(9), 'depth2', args(10), 'spName', args(11));
            tbl = CMAP.atomic_request('/api/data/sp?', payload);
        end
        
        
        function tbl = get_catalog()
            % Returns a table containing full Simons CMAP catalog of variables.
            %
            % Example
            %
            % CMAP.get_catalog();
            %
            % See also search_catalog, get_var_catalog, get_metadata, get_dataset_metadata, 
            % datasets, get_unit, get_var_resolution, get_var_coverage, get_var_stat
                        
            tbl = CMAP.query('EXEC uspCatalog');
        end
        

        function tbl = search_catalog(keywords)
            % Returns a dataframe containing a subset of Simons CMAP catalog of variables. 
            % All variables at Simons CMAP catalog are annotated with a collection of semantically related keywords. 
            % This method takes the passed keywords and returns all of the variables annotated with similar keywords.
            % The passed keywords should be separated by blank space. The search result is not sensitive to the order 
            % of keywords and is not case sensitive.
            % The passed keywords can provide any 'hint' associated with the target variables. Below are a few examples: 
            %
            % * the exact variable name (e.g. NO3), or its linguistic term (Nitrate)
            %    
            % * methodology (model, satellite ...), instrument (CTD, seaflow), or disciplines (physics, biology ...) 
            %    
            % * the cruise official name (e.g. KOK1606), or unofficial cruise name (Falkor)
            %
            % * the name of data producer (e.g Penny Chisholm) or institution name (MIT)
            %
            % If you searched for a variable with semantically-related-keywords and did not get the correct results, please let us know. 
            % We can update the keywords at any point.
            %
            %
            %
            % Example1: 
            % List of all measurements by University of Hawaii hosted by Simons CMAP.
            %
            % CMAP.search_catalog('University of Hawaii')
            %
            % Example2: 
            % Returns a list of Nitrite measurements during the Falkor cruise, if exists.
            %
            % CMAP.search_catalog('nitrite falkor')
            %
            % See also get_catalog
            
            tbl = CMAP.query(sprintf('EXEC uspSearchCatalog ''%s''', keywords));
        end
        
        
        function tbl = datasets()
            % Returns a table containing the list of data sets hosted by Simons CMAP database.
            %
            % Example
            %
            % CMAP.datasets()
            %
            % See alos get_calatlog, get_dataset_metadata

            tbl = CMAP.query('EXEC uspDatasets');
        end
        
        
        function tbl = head(tableName, rows)
            % Returns top records of a data set.
            %
            % Example
            %
            % CMAP.head('tblFalkor_2018')
            %
            % See also columns
            
            if nargin < 2
                rows = 5;
            end    
            tbl = CMAP.query(sprintf('EXEC uspHead ''%s'', ''%d''', tableName, rows));
        end

        
        function tbl = columns(tableName)
            % Returns the list of data set columns.
            %
            % Example
            %
            % CMAP.columns('tblAMT13_Chisholm')
            %
            % See also has_field, head

            tbl = CMAP.query(sprintf('EXEC uspColumns ''%s''', tableName));
        end
        
        
        function datasetID = get_dataset_ID(tableName)
            % Returns dataset ID.
            %
            % Example
            %
            % CMAP.get_dataset_ID('tblHOT_LAVA')
            %
            % See also get_catalog, get_dataset

            datasetID = CMAP.query(sprintf('SELECT DISTINCT(Dataset_ID) FROM dbo.udfCatalog() WHERE LOWER(Table_Name)=LOWER(''%s'') ', tableName)).Dataset_ID;
        end
        
        
        function tbl = get_dataset(tableName)
            % Returns the entire dataset.
            % It is not recommended to retrieve datasets with more than 100k rows using this method.
            % For large datasets, please use the 'space_time' method and retrieve the data in smaller chunks.
            % Note that this method does not return the dataset metadata. 
            % Use the 'get_dataset_metadata' method to get the dataset metadata.
            %
            % Example
            %
            % CMAP.get_dataset('tblKM1906_Gradients3_uway_optics')
            %
            % See also datasets, get_dataset_metadata                       
            
            datasetID = CMAP.get_dataset_ID(tableName);
            maxRow = 2000000;
            df = CMAP.query(sprintf('SELECT JSON_stats FROM tblDataset_Stats WHERE Dataset_ID=%d ', datasetID));
            js = jsondecode(char(df.JSON_stats(1)));
            rows = js.lat.count;
            if isempty(rows)
                error('No size estimates found for the %s table.', tableName)
            end
            if rows > maxRow
                msg = sprintf('The requested dataset has %d records.', rows);
                msg = strcat(msg, sprintf('\nIt is not recommended to retrieve datasets with more than %d rows using this method.\n', maxRow));
                msg = strcat(msg, sprintf('\nFor large datasets, please use the ''space_time'' method and retrieve the data in smaller chunks.'));
                error(msg)
            end    
            tbl = CMAP.query(sprintf("SELECT * FROM %s", tableName));
        end
        
        
        function tbl = get_dataset_metadata(tableName)
            % Returns a table containing the data set metadata.
            %
            % Example
            %
            % CMAP.get_dataset_metadata('tblArgoMerge_REP')
            %
            % See also datasets, get_dataset                       

            tbl = CMAP.query(sprintf('EXEC uspDatasetMetadata ''%s''', tableName));
        end
        
        
        function tbl = get_var_catalog(tableName, varName)
            % Returns a single-row table from catalog (udfCatalog) containing all of the variable's info at catalog.
            %
            % Example
            %
            % CMAP.get_var_catalog('tblDarwin_Ecosystem', 'phytoplankton')
            %
            % See also get_catalog

            query = sprintf('SELECT * FROM [dbo].udfCatalog() WHERE Table_Name=''%s'' AND Variable=''%s''', tableName, varName);
            tbl = CMAP.query(query);
        end
        

        function tbl = get_var_long_name(tableName, varName)
            % Returns the long name of a given variable.
            %tbl = char(CMAP.query(sprintf('EXEC uspVariableLongName ''%s'', ''%s''', tableName, varName)).Long_Name);
            %
            % Example
            %
            % CMAP.get_var_long_name('tblAltimetry_REP', 'adt')
            %
            % See also get_catalog, get_unit, get_var_resolution,
            % get_var_coverage, get_var_stat

            tbl = char(CMAP.query(sprintf('SELECT Long_Name, Short_Name FROM tblVariables WHERE Table_Name=''%s'' AND  Short_Name=''%s''', tableName, varName)).Long_Name);
        end
        

        function tbl = get_unit(tableName, varName)
            % Returns the unit for a given variable.
            %
            % Example
            %
            % CMAP.get_unit('tblHOT_ParticleFlux', 'silica_hot')
            %
            % See also get_catalog, get_var_long_name, get_var_resolution,
            % get_var_coverage, get_var_stat

            tbl = char(CMAP.query(sprintf('SELECT Unit, Short_Name FROM tblVariables WHERE Table_Name=''%s'' AND  Short_Name=''%s''', tableName, varName)).Unit);
            %tbl = char(CMAP.query(sprintf('EXEC uspVariableUnit ''%s'', ''%s''', tableName, varName)).Unit);
        end
        

        function tbl = get_var_resolution(tableName, varName)
            % Returns a single-row table from catalog (udfCatalog) containing the variable's spatial and temporal resolutions.
            %
            % Example
            %
            % CMAP.get_var_resolution('tblModis_AOD_REP', 'AOD')
            %
            % See also get_catalog, get_var_long_name, get_unit,
            % get_var_coverage, get_var_stat

            tbl = CMAP.query(sprintf('EXEC uspVariableResolution ''%s'', ''%s''', tableName, varName));
        end
        

        function tbl = get_var_coverage(tableName, varName)
            % Returns a single-row table from catalog (udfCatalog) containing the variable's spatial and temporal coverage.
            %
            % Example
            %
            % CMAP.get_var_coverage('tblCHL_REP', 'chl')
            %
            % See also get_catalog, get_var_long_name, get_unit,
            % get_var_resolution, get_var_stat

            tbl = CMAP.query(sprintf('EXEC uspVariableCoverage ''%s'', ''%s''', tableName, varName));
        end
        

        function tbl = get_var_stat(tableName, varName)
            % Returns a single-row table from catalog (udfCatalog) containing the variable's summary statistics.
            %
            % Example
            %
            % CMAP.get_var_stat('tblHOT_LAVA', 'Prochlorococcus')
            %
            % See also get_catalog, get_var_long_name, get_unit,
            % get_var_resolution, get_var_coverage

            tbl = CMAP.query(sprintf('EXEC uspVariableStat ''%s'', ''%s''', tableName, varName));
        end


        
        function hasField = has_field(tableName, varName)
            % Returns a boolean confirming whether a field (varName) exists in a table (data set).
            %
            % Example
            %
            % CMAP.has_field('tblAltimetry_REP', 'sla')
            %
            % See also columns, head

            query = sprintf('SELECT COL_LENGTH(''%s'', ''%s'') AS RESULT ', tableName, varName);
            df = CMAP.query(query).RESULT;
            hasField = false;
            if ~isempty(df) 
                hasField = true;
            end                
        end
        
        
        function grid = is_grid(tableName, varName)
            % Returns a boolean indicating whether the variable is a gridded product or has irregular spatial resolution.
            %
            % Example
            %
            % CMAP.is_grid('tblArgoMerge_REP', 'argo_merge_salinity_adj')
            %
            % See also is_climatology

            grid = true;
            query = sprintf('SELECT Spatial_Res_ID, RTRIM(LTRIM(Spatial_Resolution)) AS Spatial_Resolution FROM tblVariables JOIN tblSpatial_Resolutions ON [tblVariables].Spatial_Res_ID=[tblSpatial_Resolutions].ID WHERE Table_Name=''%s'' AND Short_Name=''%s'' ', tableName, varName);
            df = CMAP.query(query);
            if isempty(df) 
                grid = NaN;
            elseif contains(lower(char(df.Spatial_Resolution)), 'irregular')    
                grid = false;
            end            
            
        end

                
        function clim = is_climatology(tableName)
            % Returns True if the table represents a climatological data set.    
            % Currently, the logic is based on the table name.
            % TODO: Ultimately, it should query the DB to determine if it's a climatological data set.
            %
            % Example
            %
            % CMAP.is_climatology('tblDarwin_Plankton_Climatology')
            %
            % See also is_grid

            clim = contains(tableName, '_Climatology');
        end
                
       
        function tbl = get_references(datasetID)
            % Returns a table containing refrences associated with a data set.
            %
            % Example
            %
            % CMAP.get_references(21)
            %
            % See also get_dataset_metadata, datasets, get_dataset

            tbl = CMAP.query(sprintf('SELECT Reference FROM dbo.udfDatasetReferences(%d)', datasetID));
        end
        
        
        function tbl = get_metadata(table, variable)
            % Returns a table containing the variable metadata.
            %
            % Example
            %
            % CMAP.get_metadata('tblsst_AVHRR_OI_NRT', 'sst')
            %
            % See also get_dataset_metadata, get_catalog

            tbl = CMAP.query(sprintf('EXEC uspVariableMetaData ''%s'', ''%s''', table, variable));
        end
        
        
        function tbl = cruises()
            % Returns a table containing a list of the hosted cruise names.
            %
            % Example
            %
            % CMAP.cruises()
            %
            % See also cruise_by_name, cruise_bounds, cruise_trajectory

            tbl = CMAP.query('EXEC uspCruises');
        end    
        

        function tbl = cruise_by_name(cruiseName)
            % Returns a table containing cruise info using cruise name.
            %
            % Example
            %
            % CMAP.cruise_by_name('diel')
            %
            % See also cruises, cruise_bounds, cruise_trajectory, cruise_variables

            tbl = CMAP.query(sprintf('EXEC uspCruiseByName ''%s''', cruiseName));
            [rows, ~] = size(tbl);
            if isempty(tbl)
                error('Invalid cruise name: %s', cruiseName);
            end
            if rows > 1
                disp(tbl)
                error('More than one cruise found. Please provide a more specific cruise name. ')
            end
        end    
        

        function tbl = cruise_bounds(cruiseName)
            % Returns a table containing cruise boundaries in space and time.
            %
            % Example
            %
            % CMAP.cruise_bounds('KOK1606')
            %
            % See also cruises, cruise_by_name, cruise_trajectory, cruise_variables

            df = CMAP.cruise_by_name(cruiseName);
            tbl = CMAP.query(sprintf('EXEC uspCruiseBounds %d', df.ID));
        end    
        
        
        function tbl = cruise_trajectory(cruiseName)
            % Returns a table containing the cruise trajectory.
            %
            % Example
            %
            % CMAP.cruise_trajectory('gradients_1')
            %
            % See also cruises, cruise_by_name, cruise_bounds, cruise_variables

            df = CMAP.cruise_by_name(cruiseName);
            tbl = CMAP.query(sprintf('EXEC uspCruiseTrajectory %d', df.ID));
        end    
        
        
        function tbl = cruise_variables(cruiseName)
            % Returns a table containing all registered variables (at Simons CMAP) during a cruise.
            %
            % Example
            %
            % CMAP.cruise_variables('SCOPE_Falkor1')
            %
            % See also cruises, cruise_by_name, cruise_bounds, cruise_trajectory

            df = CMAP.cruise_by_name(cruiseName);
            tbl = CMAP.query(sprintf('SELECT * FROM dbo.udfCruiseVariables(%d)', df.ID));
        end    

        
        function tbl = subset(spName, table, variable, dt1, dt2, lat1, lat2, lon1, lon2, depth1, depth2)
            % Returns a subset of data according to space-time constraints.
            % This methode is intended to be used internally.
            
            args = {string(table), string(variable),... 
                    string(dt1), string(dt2),...
                    string(lat1), string(lat2),...
                    string(lon1), string(lon2),...
                    string(depth1), string(depth2),...
                    string(spName)};
            tbl = CMAP.stored_proc(args);
        end

        
        function tbl = space_time(table, variable, dt1, dt2, lat1, lat2, lon1, lon2, depth1, depth2)
            % Returns a subset of data according to space-time constraints.
            % The results are ordered by time, lat, lon, and depth (if exists).
            %
            % Parameters
            %
            % :param str table: Table name (a dataset is stored in a table). 
            % A full list of table names can be found in the Catalog 
            % (see get_catalog method).
            %
            % :param str variable: Variable short name which directly corresponds 
            % to a field name in the table. A subset of this variable is returned by 
            % this method according to the spatio-temporal cut parameters (below). 
            % Pass * wild card to retrieve all fields in a table. A full list of 
            % variable short names can be found in the catalog (see get_catalog method).
            %
            % :param str dt1: Start date or datetime. This parameter sets the lower 
            % bound of the temporal cut. Example values: ?2016-05-25? or ?2017-12-10 17:25:00?
            %
            % :param str dt2: End date or datetime. This parameter sets the upper 
            % bound of the temporal cut.
            %
            % :param double lat1: Start latitude [degree N]. This parameter 
            % sets the lower bound of the meridional cut. Note latitude ranges 
            % from -90° to 90°.
            %
            % :param double lat1: End latitude [degree N]. This parameter 
            % sets the upper bound of the meridional cut. Note latitude ranges 
            % from -90° to 90°.
            %
            % :param double lon1: Start longitude [degree E]. This parameter 
            % sets the lower bound of the zonal cut. Note longitude ranges 
            % from -180° to 180°.
            %
            % :param double lon2: End longitude [degree E]. This parameter 
            % sets the upper bound of the zonal cut. Note longitude ranges 
            % from -180° to 180°.
            %
            % :param double depth1: Start depth [m]. This parameter sets the 
            % lower bound of the vertical cut. Note depth is a positive number 
            % (it is 0 at surface and grows towards ocean floor).
            %
            % :param double depth2: End depth [m]. This parameter sets the 
            % upper bound of the vertical cut. Note depth is a positive number 
            % (it is 0 at surface and grows towards ocean floor).
            %
            %
            %
            %
            % Example 1:
            % This example retrieves a subset of in-situ salinity measurements 
            % by Argo floats.
            % 
            % CMAP.space_time(...
            %                 'tblArgoMerge_REP',... % table
            %                 'argo_merge_salinity_adj',... % variable  
            %                 '2015-05-01',... % dt1  
            %                 '2015-05-30',... % dt2
            %                 28,... % lat1
            %                 38,... % lat2
            %                 -71,... % lon1
            %                 -50,... % lon2
            %                 0,... % depth1
            %                 100)  % depth2
            %        
            %
            %
            %
            % Example 2
            % This example retrieves a subset of sea surface temperature measured by satellite. 
            % Notice, depth1 and depth2 values are automatically ignored because this is a surface dataset.
            % A simple plot is made to visualize the retrieved data.
            %
            % tbl = CMAP.space_time(...
            %                       'tblsst_AVHRR_OI_NRT',... % table
            %                       'sst',... % variable  
            %                       '2016-04-30',... % dt1  
            %                       '2016-04-30',... % dt2
            %                       10,... % lat1
            %                       70,... % lat2
            %                       -180,... % lon1
            %                       -80,... % lon2
            %                       0,... % depth1
            %                       0);   % depth2
            %                 
            % lat = unique(tbl.lat);
            % lon = unique(tbl.lon);
            % sst = reshape(tbl.sst, length(lon), length(lat));
            % imagesc(lon, lat, sst');
            % axis xy;
            % title('Sea Surface Temperature');
            % colorbar();
            %
            %
            %
            % See also time_series, depth_profile, section

            tbl = CMAP.subset('uspSpaceTime', table, variable, dt1, dt2, lat1, lat2, lon1, lon2, depth1, depth2);           
        end


        function usp = interval_to_uspName(interval)
            if strcmp(interval, '')
                usp = 'uspTimeSeries';
            elseif any(strcmp(interval, {'w', 'week', 'weekly'}))    
                usp = 'uspWeekly';
            elseif any(strcmp(interval, {'m', 'month', 'monthly'}))    
                usp = 'uspMonthly';
            elseif any(strcmp(interval, {'q', 's', 'season', 'seasonal', 'seasonality', 'quarterly'}))    
                usp = 'uspQuarterly';
            elseif any(strcmp(interval, {'y', 'a', 'year', 'yearly', 'annual'}))    
                usp = 'uspAnnual';
            end                        
        end
        
        
        function tbl = time_series(table, variable, dt1, dt2, lat1, lat2, lon1, lon2, depth1, depth2, interval)
            % Returns a subset of data according to space-time constraints.
            % The results are aggregated by time and ordered by time, lat, lon, and depth (if exists).
            % The timeseries data can be binned weekyly, monthly, qurterly, or annualy, if interval variable is set (this feature is not applicable to climatological data sets). 
            %
            % Parameters
            %
            % :param str table: Table name (a dataset is stored in a table). 
            % A full list of table names can be found in the Catalog 
            % (see get_catalog method).
            %
            % :param str variable: Variable short name which directly corresponds 
            % to a field name in the table. A subset of this variable is returned by 
            % this method according to the spatio-temporal cut parameters (below). 
            % Pass * wild card to retrieve all fields in a table. A full list of 
            % variable short names can be found in the catalog (see get_catalog method).
            %
            % :param str dt1: Start date or datetime. This parameter sets the lower 
            % bound of the temporal cut. Example values: ?2016-05-25? or ?2017-12-10 17:25:00?
            %
            % :param str dt2: End date or datetime. This parameter sets the upper 
            % bound of the temporal cut.
            %
            % :param double lat1: Start latitude [degree N]. This parameter 
            % sets the lower bound of the meridional cut. Note latitude ranges 
            % from -90° to 90°.
            %
            % :param double lat1: End latitude [degree N]. This parameter 
            % sets the upper bound of the meridional cut. Note latitude ranges 
            % from -90° to 90°.
            %
            % :param double lon1: Start longitude [degree E]. This parameter 
            % sets the lower bound of the zonal cut. Note longitude ranges 
            % from -180° to 180°.
            %
            % :param double lon2: End longitude [degree E]. This parameter 
            % sets the upper bound of the zonal cut. Note longitude ranges 
            % from -180° to 180°.
            %
            % :param double depth1: Start depth [m]. This parameter sets the 
            % lower bound of the vertical cut. Note depth is a positive number 
            % (it is 0 at surface and grows towards ocean floor).
            %
            % :param double depth2: End depth [m]. This parameter sets the 
            % upper bound of the vertical cut. Note depth is a positive number 
            % (it is 0 at surface and grows towards ocean floor).
            %
            % :param str interval: The timeseries bin size. If '', the native 
            % dataset time resolution is used as the bin size. Below is a list 
            % of interval values for other binning options: 
            % * ?w? or ?week? for weekly timeseries. 
            % * ?m? or ?month? for monthly timeseries. 
            % * ?s? or ?q? for seasonal/quarterly timeseries. 
            % * ?a? or ?y? for annual/yearly timeseries.
            %
            %
            %
            %
            % Example 1:
            % This example retrieves the timeseries of SiO4 measurements 
            % conducted by HOT team at University of Hawaii, spanning from 
            % 1988 to 2016.             
            % 
            % CMAP.time_series(...
            %                 'tblHOT_Bottle',... % table
            %                 'SiO4_bottle_hot',... % variable  
            %                 '1988-12-01',... % dt1  
            %                 '2016-10-15',... % dt2
            %                 22,... % lat1
            %                 23,... % lat2
            %                 -159,... % lon1
            %                 -157,... % lon2
            %                 0,... % depth1
            %                 200)  % depth2
            %        
            %
            %
            %
            % Example 2
            % This example retrieves a 24-year long timeseries of 
            % absolute dynamic topography (closely related to sea 
            % surface height) measured by satellite.
            % Notice, depth1 and depth2 values are automatically ignored 
            % because this is a surface dataset. The 'interval' parameter 
            % has set to 'y' indicating yearly binning (inter-annual timeseres). 
            % This example takes a few moments to run as the altimetry dataset 
            % is very large (multi-decade daily-global remote sensing).
            % The last few lines of code makes a simple plot to visualize 
            % the retrieved data.
            %
            %
            % tbl = CMAP.time_series(...
            %                       'tblAltimetry_REP',... % table
            %                       'adt',... % variable  
            %                       '1994-01-01',... % dt1  
            %                       '2017-12-31',... % dt2
            %                       30,... % lat1
            %                       32,... % lat2
            %                       -160,... % lon1
            %                       -158,... % lon2
            %                       0,... % depth1
            %                       0,... % depth2
            %                       'y'); % interval
            %                 
            % errorbar(tbl.year, tbl.adt, tbl.adt_std);
            % xlabel('Year');
            % ylabel('Absolute Dynamic Topography (m)');
            %
            %
            %
            % See also space_time, depth_profile, section
            
            if nargin < 11
                interval = '';
            end               
            uspName = CMAP.interval_to_uspName(interval);            
            if ~strcmp(uspName, 'uspTimeSeries') && CMAP.is_climatology(table)
                error('\nTable %s represents a climatological data set. \n%s', table, 'Custom binning (monthly, weekly, ...) is not suppoerted for climatological data sets. ')
            end    
            tbl = CMAP.subset(uspName, table, variable, dt1, dt2, lat1, lat2, lon1, lon2, depth1, depth2);           
        end
        
        
        function tbl = depth_profile(table, variable, dt1, dt2, lat1, lat2, lon1, lon2, depth1, depth2)
            % Returns a subset of data according to space-time constraints.
            % The results are aggregated by depth and ordered by depth.
            %
            % Parameters
            %
            % :param str table: Table name (a dataset is stored in a table). 
            % A full list of table names can be found in the Catalog 
            % (see get_catalog method).
            %
            % :param str variable: Variable short name which directly corresponds 
            % to a field name in the table. A subset of this variable is returned by 
            % this method according to the spatio-temporal cut parameters (below). 
            % Pass * wild card to retrieve all fields in a table. A full list of 
            % variable short names can be found in the catalog (see get_catalog method).
            %
            % :param str dt1: Start date or datetime. This parameter sets the lower 
            % bound of the temporal cut. Example values: ?2016-05-25? or ?2017-12-10 17:25:00?
            %
            % :param str dt2: End date or datetime. This parameter sets the upper 
            % bound of the temporal cut.
            %
            % :param double lat1: Start latitude [degree N]. This parameter 
            % sets the lower bound of the meridional cut. Note latitude ranges 
            % from -90° to 90°.
            %
            % :param double lat1: End latitude [degree N]. This parameter 
            % sets the upper bound of the meridional cut. Note latitude ranges 
            % from -90° to 90°.
            %
            % :param double lon1: Start longitude [degree E]. This parameter 
            % sets the lower bound of the zonal cut. Note longitude ranges 
            % from -180° to 180°.
            %
            % :param double lon2: End longitude [degree E]. This parameter 
            % sets the upper bound of the zonal cut. Note longitude ranges 
            % from -180° to 180°.
            %
            % :param double depth1: Start depth [m]. This parameter sets the 
            % lower bound of the vertical cut. Note depth is a positive number 
            % (it is 0 at surface and grows towards ocean floor).
            %
            % :param double depth2: End depth [m]. This parameter sets the 
            % upper bound of the vertical cut. Note depth is a positive number 
            % (it is 0 at surface and grows towards ocean floor).
            %
            %
            %
            %
            % Example 1:
            % This example retrieves a depth profile of in-situ chlorophyll 
            % concentration measurements by Argo Floats. The last few lines 
            % of code a simple plot showing the chlorophyll depth profile 
            % (deep chlorophyll maximum near 100 m).
            % 
            % tbl = CMAP.depth_profile(...
            %                          'tblArgoMerge_REP',... % table
            %                          'argo_merge_chl_adj',... % variable  
            %                          '2016-04-30',... % dt1  
            %                          '2016-04-30',... % dt2
            %                          20,...   % lat1
            %                          24,...   % lat2
            %                          -170,... % lon1
            %                          -150,... % lon2
            %                          0,...    % depth1
            %                          1500);   % depth2
            %        
            % plot(tbl.depth, tbl.argo_merge_chl_adj, 'o');
            % xlabel('depth [m]');
            % ylabel(sprintf('%s [%s]', 'argo chl adjusted', CMAP.get_unit('tblArgoMerge_REP', 'argo_merge_chl_adj')));
            %
            %
            %
            % Example 2
            % This example retrieves depth profile of modeled chlorophyll 
            % concentration estimated by Pisces, a weekly 0.5° resolution 
            % BioGeoChemical model. The last few lines of code creates a 
            % simple plot showing the chlorophyll depth profile. 
            % The deep chlorophyll maximum (DCM) is approximately near 
            % ~100 m, closely matching the in-situ observations by ARGO Floats 
            % (see the previous example).
            %
            % tbl = CMAP.depth_profile(...
            %                          'tblPisces_NRT',... % table
            %                          'CHL',... % variable  
            %                          '2016-04-30',... % dt1  
            %                          '2016-04-30',... % dt2
            %                          20,...   % lat1
            %                          24,...   % lat2
            %                          -170,... % lon1
            %                          -150,... % lon2
            %                          0,...    % depth1
            %                          1500);   % depth2
            %                 
            % plot(tbl.depth, tbl.CHL, 'o');
            % xlabel('depth [m]');
            % ylabel(sprintf('%s [%s]', 'CHL', CMAP.get_unit('tblPisces_NRT', 'CHL')));
            %
            %
            %
            % See also space_time, time_series, section

            tbl = CMAP.subset('uspDepthProfile', table, variable, dt1, dt2, lat1, lat2, lon1, lon2, depth1, depth2);           
        end

        
        function tbl = section(table, variable, dt1, dt2, lat1, lat2, lon1, lon2, depth1, depth2)
            % Returns a subset of data according to space-time constraints.
            % The results are ordered by time, lat, lon, and depth.
            %
            % Parameters
            %
            % :param str table: Table name (a dataset is stored in a table). 
            % A full list of table names can be found in the Catalog 
            % (see get_catalog method).
            %
            % :param str variable: Variable short name which directly corresponds 
            % to a field name in the table. A subset of this variable is returned by 
            % this method according to the spatio-temporal cut parameters (below). 
            % Pass * wild card to retrieve all fields in a table. A full list of 
            % variable short names can be found in the catalog (see get_catalog method).
            %
            % :param str dt1: Start date or datetime. This parameter sets the lower 
            % bound of the temporal cut. Example values: ?2016-05-25? or ?2017-12-10 17:25:00?
            %
            % :param str dt2: End date or datetime. This parameter sets the upper 
            % bound of the temporal cut.
            %
            % :param double lat1: Start latitude [degree N]. This parameter 
            % sets the lower bound of the meridional cut. Note latitude ranges 
            % from -90° to 90°.
            %
            % :param double lat1: End latitude [degree N]. This parameter 
            % sets the upper bound of the meridional cut. Note latitude ranges 
            % from -90° to 90°.
            %
            % :param double lon1: Start longitude [degree E]. This parameter 
            % sets the lower bound of the zonal cut. Note longitude ranges 
            % from -180° to 180°.
            %
            % :param double lon2: End longitude [degree E]. This parameter 
            % sets the upper bound of the zonal cut. Note longitude ranges 
            % from -180° to 180°.
            %
            % :param double depth1: Start depth [m]. This parameter sets the 
            % lower bound of the vertical cut. Note depth is a positive number 
            % (it is 0 at surface and grows towards ocean floor).
            %
            % :param double depth2: End depth [m]. This parameter sets the 
            % upper bound of the vertical cut. Note depth is a positive number 
            % (it is 0 at surface and grows towards ocean floor).
            %
            %
            %
            %
            % Example
            % This example retrieves depth profile of modeled dissolved Nitrate 
            % concentration estimated by Pisces, a weekly 0.5° resolution 
            % BioGeoChemical model. The last few lines of code creates a 
            % simple plot showing dissolved NO3 section map. 
            %
            % tbl = CMAP.section(...
            %                    'tblPisces_NRT',... % table
            %                    'NO3',... % variable  
            %                    '2016-04-30',... % dt1  
            %                    '2016-04-30',... % dt2
            %                    10,...   % lat1
            %                    50,...   % lat2
            %                    -158,... % lon1
            %                    -158,... % lon2
            %                    0,...    % depth1
            %                    100);    % depth2
            %                 
            % lat = unique(tbl.lat);
            % depth = unique(tbl.depth);
            % [LAT, DEPTH] = meshgrid(lat, depth);
            % NO3 = reshape(tbl.NO3, length(depth), length(lat));
            % contourf(LAT, DEPTH, NO3);
            % colorbar();
            % xlabel('latitude');
            % ylabel('depth [m]');
            % title(sprintf('%s [%s]', 'Dissolved NO3 Concentration', CMAP.get_unit('tblPisces_NRT', 'NO3')));
            % set(gca, 'YDir','reverse');
            % set(gcf,'position',[10,10, 800, 300])
            %
            %
            % See also space_time, time_series, depth_profile

            tbl = CMAP.subset('uspSectionMap', table, variable, dt1, dt2, lat1, lat2, lon1, lon2, depth1, depth2);           
        end
        
        

        function tbl = match(sourceTable, sourceVar, targetTables, targetVars,... 
             dt1, dt2, lat1, lat2, lon1, lon2, depth1, depth2,... 
             temporalTolerance, latTolerance, lonTolerance, depthTolerance)        
            % Colocalizes the source variable (from source table) with the target variable (from target table).
            % The tolerance parameters set the matching boundaries between the source and target data sets. 
            % Returns a table containing the source variable joined with the target variable.
            %
            % Parameters
            %
            % :param str sourceTable: Table name (a dataset is stored in a table). 
            % A full list of table names can be found in the Catalog 
            % (see get_catalog method).
            %
            % :param str sourceVariable: Variable short name which directly corresponds 
            % to a field name in the table. A subset of this variable is returned by 
            % this method according to the spatio-temporal cut parameters (below). 
            % Pass * wild card to retrieve all fields in a table. A full list of 
            % variable short names can be found in the catalog (see get_catalog method).
            %
            % :param cell array targetTables: table names of the target data sets 
            % to be matched with the source data.
            %
            % :param cell array targetVariables: variable names to be matched with 
            % the source variable.
            %
            % :param str dt1: Start date or datetime. This parameter sets the lower 
            % bound of the temporal cut. Example values: ?2016-05-25? or ?2017-12-10 17:25:00?
            %
            % :param str dt2: End date or datetime. This parameter sets the upper 
            % bound of the temporal cut.
            %
            % :param double lat1: Start latitude [degree N]. This parameter 
            % sets the lower bound of the meridional cut. Note latitude ranges 
            % from -90° to 90°.
            %
            % :param double lat1: End latitude [degree N]. This parameter 
            % sets the upper bound of the meridional cut. Note latitude ranges 
            % from -90° to 90°.
            %
            % :param double lon1: Start longitude [degree E]. This parameter 
            % sets the lower bound of the zonal cut. Note longitude ranges 
            % from -180° to 180°.
            %
            % :param double lon2: End longitude [degree E]. This parameter 
            % sets the upper bound of the zonal cut. Note longitude ranges 
            % from -180° to 180°.
            %
            % :param double depth1: Start depth [m]. This parameter sets the 
            % lower bound of the vertical cut. Note depth is a positive number 
            % (it is 0 at surface and grows towards ocean floor).
            %
            % :param double depth2: End depth [m]. This parameter sets the 
            % upper bound of the vertical cut. Note depth is a positive number 
            % (it is 0 at surface and grows towards ocean floor).
            %
            % :param cell array timeTolerance: float list of temporal tolerance 
            % values between pairs of source and target datasets. The size and 
            % order of values in this list should match those of targetTables. 
            % This parameter is in day units except when the target variable 
            % represents monthly climatology data in which case it is in month units. 
            % Notice fractional values are not supported in the current version.
            %
            % :param cell array latTolerance: float list of spatial tolerance values 
            % in meridional direction [deg] between pairs of source and target data 
            % sets.
            %
            % :param cell array lonTolerance: float list of spatial tolerance values 
            % in zonal direction [deg] between pairs of source and target data sets. 
            %
            % :param cell array depthTolerance: float list of spatial tolerance values 
            % in vertical direction [m] between pairs of source and target data sets.
            %
            %
            %
            %
            % Example
            % The source variable in this example is particulate pseudo 
            % cobalamin (?Me_PseudoCobalamin_Particulate_pM?) measured by 
            % Ingalls lab during the KM1315 cruise. This variable is 
            % colocalized with one target variabele, ?picoprokaryote? concentration, 
            % from Darwin model. The colocalized data, then is visualized. 
            %
            % CMAP.match(...
            %            'tblKM1314_Cobalmins',...                % sourceTable  
            %             'Me_PseudoCobalamin_Particulate_pM',... % sourceVariable 
            %              {'tblDarwin_Phytoplankton'},...  % targetTables
            %              {'picoprokaryote'},...           % targetVariables                    
            %              '2013-08-11',... % dt1
            %              '2013-09-05',... % dt2
            %               22.25,...       % lat1
            %               450.25,...      % lat2
            %               -159.25,...     % lon1
            %                -127.75,...    % lon2     
            %                -5,...         % depth1
            %                305,...        % depth2
            %                {1},...        % timeTolerance
            %                {0.25},...     % latTolerance 
            %                {0.25},...     % lonTolerance     
            %                {5});          % depthTolerance
            %                 
            %
            % See also along_track

            tbl = Match('uspMatch', sourceTable, sourceVar, targetTables, targetVars,...
                     dt1, dt2, lat1, lat2, lon1, lon2, depth1, depth2,...
                     temporalTolerance, latTolerance, lonTolerance, depthTolerance).compile();
        end

        
        

        function tbl = along_track(cruise, targetTables, targetVars, depth1, depth2, temporalTolerance, latTolerance, lonTolerance, depthTolerance)     
            % Takes a cruise name and colocalizes the cruise track with the specified variable(s).
            %
            % Parameters
            %
            % :param str cruise: The official cruise name. If applicable, 
            % you may also use cruise ?nickname? (?Diel?, ?Gradients_1? ?). 
            % A full list of cruise names can be retrieved using the 'cruises()' method.
            %
            % :param cell array targetTables: table names of the target data sets 
            % to be matched with the source data.
            %
            % :param cell array targetVariables: variable names to be matched with 
            % the source variable.
            %
            % :param double depth1: Start depth [m]. This parameter sets the 
            % lower bound of the vertical cut. Note depth is a positive number 
            % (it is 0 at surface and grows towards ocean floor).
            %
            % :param double depth2: End depth [m]. This parameter sets the 
            % upper bound of the vertical cut. Note depth is a positive number 
            % (it is 0 at surface and grows towards ocean floor).
            %
            % :param cell array timeTolerance: float list of temporal tolerance 
            % values between pairs of source and target datasets. The size and 
            % order of values in this list should match those of targetTables. 
            % This parameter is in day units except when the target variable 
            % represents monthly climatology data in which case it is in month units. 
            % Notice fractional values are not supported in the current version.
            %
            % :param cell array latTolerance: float list of spatial tolerance values 
            % in meridional direction [deg] between pairs of source and target data 
            % sets.
            %
            % :param cell array lonTolerance: float list of spatial tolerance values 
            % in zonal direction [deg] between pairs of source and target data sets. 
            %
            % :param cell array depthTolerance: float list of spatial tolerance values 
            % in vertical direction [m] between pairs of source and target data sets.
            %
            %
            %
            %
            % Example
            % This example demonstrates how to colocalize the ?gradients_1? 
            % cruise (official name: KOK1606) with 2 target variables:
            % ?prochloro_abundance? from underway seaflow dataset
            % ?PO4? from Darwin climatology model
            % The last few lines of this snippet plots the colocalized 
            % synecho_abundance versus NO3 concentration.
            %
            %
            % tbl = CMAP.along_track(...
            %                       'gradients_1',...                % cruise  
            %                       {'tblSeaFlow', 'tblDarwin_Nutrient_Climatology'},...  % targetTables
            %                       {'prochloro_abundance', 'PO4_darwin_clim'},...           % targetVariables                    
            %                       0,...                % depth1
            %                       5,...                % depth2
            %                       {0, 0},...           % timeTolerance
            %                       {0.01, 0.25},...     % latTolerance 
            %                       {0.01, 0.25},...     % lonTolerance     
            %                       {0, 5});             % depthTolerance
            %
            % yyaxis left;
            % plot(tbl.lat, tbl.prochloro_abundance, 'o');
            % ylabel(sprintf('Prochlorococcus Abundance [%s]', CMAP.get_unit('tblSeaflow', 'prochloro_abundance')));
            % yyaxis right;
            % plot(tbl.lat, tbl.PO4_darwin_clim, 'o');
            % xlabel('latitude');  
            % ylabel(sprintf('PO4 Darwin Climatology [%s]', CMAP.get_unit('tblDarwin_Nutrient_Climatology', 'PO4_darwin_clim')));
            %
            % See also match

            df = CMAP.cruise_bounds(cruise);
            tbl = CMAP.match(...
                             'tblCruise_Trajectory',...       % sourceTable
                             string(df.ID(1)),...             % sourceVar
                             targetTables,...                 % targetTables
                             targetVars,...                   % targetVars
                             df.dt1(1),...                    % dt1
                             df.dt2(1),...                    % dt2
                             df.lat1(1),...                   % lat1
                             df.lat2(1),...                   % lat2
                             df.lon1(1),...                   % lon1
                             df.lon2(1),...                   % lon2
                             depth1,...                       % depth1
                             depth2,...                       % depth2
                             temporalTolerance,...            % temporalTolerance
                             latTolerance,...                 % latTolerance
                             lonTolerance,...                 % lonTolerance
                             depthTolerance...                % depthTolerance
                             );
        end        
    end
end

