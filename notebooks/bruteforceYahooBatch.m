%
% Copyright 2020 Renjie Wu
%
% Licensed under the Apache License, Version 2.0 (the "License");
% you may not use this file except in compliance with the License.
% You may obtain a copy of the License at
%
%     http://www.apache.org/licenses/LICENSE-2.0
%
% Unless required by applicable law or agreed to in writing, software
% distributed under the License is distributed on an "AS IS" BASIS,
% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
% See the License for the specific language governing permissions and
% limitations under the License.
%

function bruteforceYahooBatch(A1A2, folder, outputPath)
    csvFiles = dir(fullfile(folder, '*.csv'));
    
    fid = fopen(outputPath, 'w');
    fprintf(fid, "filename, u, k, c, b, mismatch\n");
    
    for i = 1:length(csvFiles)
        filename = fullfile(folder, csvFiles(i).name);
        fprintf("%s: ", csvFiles(i).name);
        [u, k, c, b, mismatch] = bruteforceYahoo(A1A2, filename);
        
        fprintf(fid, "%s, %d, %d, %d, %.2f, %d\n", csvFiles(i).name, u, k, c, b, mismatch);
    end
    
    fclose(fid);
end

function [bestU, bestK, bestC, bestB, bestMismatch] = bruteforceYahoo(A1A2, csvPath)
    CSV = csvread(csvPath, 1, 0);
    TS = CSV(:, 2);
    truthAnomaly = CSV(:, 3);
    
    data = (A1A2 == 1) * abs(diff(TS)) + (A1A2 ~= 1) * diff(TS);
    dataMin = min(data);
    dataMax = max(data);
    
    bestMismatch = length(TS);
    delta = 2;
    
    % Compute intervals
    truthAnomalyLoc = computeInterval(truthAnomaly);
    
    % So diff(TS)>b to be tested first
    for k = [1, 5:5:length(TS)]
        avg = movmean(data, k);
        std = movstd(data, k);
        
        for c = [0, 0:0.05:3]
            y = (k ~= 1) * avg + c * std;
            
            if dataMax <= 5 
                b_range = dataMin:0.05:dataMax;
            else
                b_range = floor(dataMin / 5) * 5:5:ceil(dataMax / 5) * 5;
            end
            
            for b = b_range
                detectAnomaly = data > y + b;
                detectAnomalyLoc = computeInterval(detectAnomaly);
                
                mismatch = 0;
                i = 1; j = 1;
                
                while i <= size(truthAnomalyLoc, 1) && j <= size(detectAnomalyLoc, 1)
                    if truthAnomalyLoc(i, 1) - delta <= detectAnomalyLoc(j, 1) && truthAnomalyLoc(i, 2) + delta >= detectAnomalyLoc(j, 2) 
                        i = i + 1;
                        j = j + 1;
                    elseif truthAnomalyLoc(i, 1) - delta > detectAnomalyLoc(j, 1)
                        mismatch = mismatch + 1;
                        j = j + 1;
                    else % truthAnomalyLoc(i, 2) + delta < detectAnomalyLoc(i, 2)
                        mismatch = mismatch + 1;
                        i = i + 1;
                    end
                end
                
                while i <= size(truthAnomalyLoc, 1)
                    mismatch = mismatch + 1;
                    i = i + 1;
                end
                
                while j <= size(detectAnomalyLoc, 1)
                    mismatch = mismatch + 1;
                    j = j + 1;
                end

                if mismatch < bestMismatch
                    bestMismatch = mismatch;
                    
                    bestU = k ~= 1;
                    bestK = k;
                    bestC = c;
                    bestB = b;
                    
                    if mismatch == 0
                        fprintf("Found solution! u = %d, k = %d, c = %d, b = %d\n", ...
                            bestU, bestK, bestC, bestB);
                        return;
                    end
                end
            end
        end
    end
    
    fprintf("So far, bestU = %d, bestK = %d, bestC = %d, bestB = %d, bestMismatch = %d\n", ...
        bestU, bestK, bestC, bestB, bestMismatch);
end

function intervalLoc = computeInterval(anomaly)
    i = 1; j = 1; 
    intervalLoc = zeros(length(anomaly), 2);
    while i <= length(anomaly)
        if anomaly(i) == 1
            start = i;

            while i <= length(anomaly) && anomaly(i) == 1
                i = i + 1;
            end

            intervalLoc(j, :) = [start i];
            j = j + 1;
        else
            i = i + 1;
        end
    end
    intervalLoc(j:end, :) = [];
end