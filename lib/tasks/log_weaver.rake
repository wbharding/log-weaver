require 'active_record'
require File.join(File.dirname(__FILE__), '../log_weaver_task_util')

include LogWeaverTaskUtil

# -----------------------------------------------------------------------------
namespace :log_weaver do
	# Only requirement is for $1 to be the date and time of this action
	NEW_ACTION_REGEX = /Processing.*?(2010[^\)]*).*/ # e.g. Processing SiteHelpController#index (for 127.0.0.1 at 2010-02-21 13:30:54) [GET] on PID 10795
	SIZE_OF_HASH_BLOCKS = 10000 # Bigger number = faster parsing, but more memory.
	MAX_TIME_BETWEEN_CONTROLLER_ACTIONS = 10.minutes

	# ---------------------------------------------------------------------------
	task :weave_logs => :environment do
		begin
			logger = setup_logger(:log_name => 'log_weaver')
			config_file = YAML::load(File.open(File.dirname(__FILE__) + "/../../config/system_logs.yml"))
			working_directory = File.dirname(__FILE__) + "/../../log/working"
			merge_files, action_count = {}, 0

			unified_log = File.open(config_file["path_to_unified_log"], 'a+')
			logger.error("Couldn't open unified log at #{config_file["path_to_unified_log"]} for appending.") unless unified_log
			log_ended_at = get_time_of_last_file_action(unified_log) || nil

			# Grab logfiles from remote hosts and put them in our working directory
			config_file["hosts_to_unify"].each_pair do |host_name, host_values|
				host_name_sym = host_name.to_sym
				host_ip, path_to_logfile = host_values["address"], host_values["log_location"]
				# Working log file formatted a la working_1_2_3_4_production.log
				working_log_name = "working_#{host_ip.gsub('.', '_')}_" + File.basename(path_to_logfile)

				rsync_input = %{rsync -Pa #{host_ip}:#{path_to_logfile} #{working_directory}/#{working_log_name} > transfer_output.txt} # hack: pipe output to avoid "pipe broken" errors
				logger.info("Beginning rsync with #{host_ip} at #{Time.now.strftime("%X")}: #{rsync_input}.")
				system rsync_input
				logger.info("Finished rsync with #{host_ip} at #{Time.now.strftime("%X")}.")

				begin
					merge_files[host_name_sym] = File.open("#{working_directory}/#{working_log_name}", 'r')
					logger.info("Opened #{working_directory}/#{working_log_name} for reading.")
				rescue
					logger.info("Error opening #{working_directory}/#{working_log_name} for reading.")
				end

				if merge_files[host_name_sym] && log_ended_at
					file_seek_to_time(merge_files[host_name_sym], log_ended_at, logger)
					logger.info("Starting file #{host_name_sym} at pos #{merge_files[host_name_sym].pos}, closest we can find to when unified files info stops.") 
				end
			end

			hashed_entries = config_file["hosts_to_unify"].keys.inject({}) do |h, k|
				build_file_into_master_hash(h, k.to_sym, merge_files[k.to_sym])
			end

			# Loop through all entries, adding them in chronlogical order to unified_log file until all actions have been exhausted
			while(true)
				target_key = get_earliest_action_host_key(hashed_entries)
				break unless target_key
				unified_log << hashed_entries[target_key][:entries].shift[:action]
				action_count += 1

				# If this host file has run out of actions, then look for more entries in its source file...
				if hashed_entries[target_key][:entries].empty?
					logger.info("Source file associated with #{target_key} has run out of actions at #{Time.now.strftime("%X")}.  Total actions run: #{action_count}.  Now trying to repopulate it.")
					hashed_entries = build_file_into_master_hash(hashed_entries, target_key, merge_files[target_key])
				end

				if(unified_log.pos > config_file['maximum_unified_log_size'])
					unified_log = transition_unified_files(unified_log)
				end
			end

			# Now close the unified log, and resave our host file information to the DB so next we rsync we know where to
			# start within the host file...
			logger.info("Finished unifying log at #{Time.now.strftime("%X")}.")
			unified_log.close
		rescue Exception => e
			logger.error(e.to_s + "\n" + e.backtrace.join("\n"))
		ensure
			merge_files.values.each { |f| f.close }
		end
	end

	# -----------------------------------------------------------------------------
	# When our existing file exceeds a reasonable size, move it to a new filename, and open a new file that uses
	# the same name as our old file did.
	def transition_unified_files(file)
		file.close
		base_name, path_name, idx = File.basename(file.path, '.log'), File.dirname(file.path), 2
		while(true)
			new_filepath = path_name + "/" + base_name + "_#{idx}.log"
			if(!File.exists?(new_filepath))
				FileUtils.mv(file.path, new_filepath) # move our old file to an unused filename
				file = File.open(path_name + "/" + base_name + ".log", 'a+') # and re-open a new file with the same name as our old file
				break
			end
			idx += 1
		end
		file
	end

	# -----------------------------------------------------------------------------
	# Save the contents of source file into a master_hash hash with the given key, so long
	# as the source file has more action entries in it.  Otherwise, remove key from master_hash
	# if the source file has been emptied. 
	def build_file_into_master_hash(master_hash, key, source_file)
		entries = grab_and_hashify(source_file, SIZE_OF_HASH_BLOCKS)
		if(!entries.blank?)
			master_hash.merge({ key.to_sym => entries })
		else
			master_hash.delete(key.to_sym)
			master_hash
		end
	end

	# -----------------------------------------------------------------------------
	def get_earliest_action_host_key(hashed_entries)
		res = hashed_entries.keys.collect{ |key| {:time => hashed_entries[key][:entries].first[:time], :key => key }}.min { |a,b| a[:time] <=> b[:time] }
		res && res[:key]
	end

	# -----------------------------------------------------------------------------
	# Grab the time of the next action after our current file position, then return to that position
	def get_next_action_time(file)
		start_pos = file.pos
		next_time = get_next_result_time(file)
		file.seek(start_pos, IO::SEEK_SET)
		next_time
	end

	# -----------------------------------------------------------------------------
	# Read backwards through file until we find the time of the last action written to it.
	def get_next_result_time(file)
		result_time = nil
		while(!file.eof? && (this_line = file.readline))
			if(this_line =~NEW_ACTION_REGEX)
				result_time = Time.parse($1)
				break
			end
		end
		result_time
	end

	# -----------------------------------------------------------------------------
	# Read backwards through file until we find the time of the last action written to it.
	def get_time_of_last_file_action(file)
		result_time = nil
		starting_pos = file.pos
		file.seek(0, IO::SEEK_END)
		file.seek(file.pos-[10000, file.pos].min, IO::SEEK_SET)
		
		while(!file.eof? && (this_line = file.readline))
			result_time = Time.parse($1) if(this_line =~NEW_ACTION_REGEX)
		end

		# Reset file
		file.seek(starting_pos, IO::SEEK_SET)
		result_time
	end

	# -----------------------------------------------------------------------------
	# This is hell slow.  If it must be used (which I question), it will need some TLC.  Would like to do
	# binary search, but it'd be a pain to write dual cases for determining the right action when moving forward
	# vs moving backwards toward it. 
	def file_seek_to_time(file, time_obj, logger)
		file.seek(0, IO::SEEK_END)
		end_pos = file.pos
		file.rewind
		file = seek_to_time_helper(file, time_obj, 0, end_pos, logger)
		starting_line_pos = file.pos

		file.each do |line|
			if(line =~ NEW_ACTION_REGEX)
				this_time = Time.parse($1)
				if this_time >= time_obj
					file.seek(starting_line_pos, IO::SEEK_SET)
					break
				end
			end
			starting_line_pos = file.pos
		end
		file
	end

	# -----------------------------------------------------------------------------
	# Do a binary search until we find a file position that is less than MAX_TIME_BETWEEN_CONTROLLER_ACTIONS
	# before the time we seek.
	def seek_to_time_helper(file, target_time, start_pos, end_pos, logger)
		return nil unless file && target_time
		logger.info("Checking between file pos #{start_pos} and #{end_pos}.")

		rewind_pos = file.pos
		middle_pos = (start_pos+end_pos)/2
		file.seek(middle_pos, IO::SEEK_SET)
		file.each do |line|
			if(line =~ NEW_ACTION_REGEX)
				this_time = Time.parse($1)
				if this_time >= target_time
					return seek_to_time_helper(file, this_time, start_pos, middle_pos, logger)
				elsif(this_time < target_time && (this_time-target_time).abs < MAX_TIME_BETWEEN_CONTROLLER_ACTIONS)
					file.seek(rewind_pos, IO::SEEK_SET)
					return file
				else
					return seek_to_time_helper(file, this_time, middle_pos, end_pos, logger)
				end
			end
		end

		# If we can't find any line that meets the criteria we seek, just send back the file object in the closest spot we could find.
		file.seek(rewind_pos, IO::SEEK_SET)
		return file
	end

	# -----------------------------------------------------------------------------
	# Return data in format:
	# { :ending_byte => ending_byte_index, :entries => [{:time => datetime, :action => action text}, ...]
	def grab_and_hashify(file, entries_to_parse, options = {})
		return nil unless file && !file.eof?
		logger = options[:logger]
		return_hash = { :ending_byte => nil, :entries => [] }
		reader, entries_count, this_entry = file, 0, nil
		reader.each do |line|
			if(line =~ NEW_ACTION_REGEX)
				this_entry = { :time => Time.parse($1), :action => line }
				logger.info("Adding action starting at #{this_entry[:time].strftime("%X")}. Action count #{entries_count}") if logger  
				entries_count += 1
				return_hash[:entries] << this_entry
			elsif(this_entry)
				this_entry[:action] += line
			end

			break if entries_count >= entries_to_parse
		end

		return_hash.merge(:ending_byte => reader.pos)
	end
end
